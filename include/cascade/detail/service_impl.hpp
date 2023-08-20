#include <algorithm>
#include <cascade/config.h>
#include <cascade/data_flow_graph.hpp>
#include <cassert>
#include <chrono>
#include <derecho/core/derecho.hpp>
#include <derecho/core/derecho_exception.hpp>
#include <derecho/core/detail/rpc_utils.hpp>
#include <derecho/core/notification.hpp>
#include <iostream>
#include <map>
#include <string>
#include <typeindex>
#include <variant>
#include <vector>
#include <regex>

using namespace std::chrono_literals;

#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

namespace derecho {
namespace cascade {

using derecho::CrossProductPolicy;
using derecho::ShardAllocationPolicy;
using derecho::SubgroupAllocationPolicy;

template <typename CascadeType>
derecho::Factory<CascadeType> factory_wrapper(ICascadeContext* context_ptr, derecho::cascade::Factory<CascadeType> cascade_factory) {
    return [context_ptr, cascade_factory](persistent::PersistentRegistry* pr, subgroup_id_t subgroup_id) {
        return cascade_factory(pr, subgroup_id, context_ptr);
    };
}

template <typename... CascadeTypes>
Service<CascadeTypes...>::Service(const std::vector<DeserializationContext*>& dsms,
                                  derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_service_factory,
                                  derecho::cascade::Factory<CascadeTypes>... factories) {
    // STEP 1 - load configuration
    derecho::SubgroupInfo si{derecho::make_subgroup_allocator<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>()};
    // STEP 2 - setup cascade context
    context = std::make_unique<CascadeContext<CascadeTypes...>>();
    std::vector<DeserializationContext*> new_dsms(dsms);
    new_dsms.emplace_back(context.get());
    // STEP 3 - create derecho group
    group = std::make_unique<derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>>(
            UserMessageCallbacks{
#ifdef ENABLE_EVALUATION
                    nullptr,
                    nullptr,
                    // persistent
                    [this](subgroup_id_t sgid, persistent::version_t ver) {
                        TimestampLogger::log(TLT_PERSISTED, group->get_my_id(), 0, get_walltime(), ver);
                    },
                    nullptr
#endif
            },
            si,
            new_dsms,
            std::vector<derecho::view_upcall_t>{},
            factory_wrapper(context.get(), metadata_service_factory),
            factory_wrapper(context.get(), factories)...);
    dbg_default_trace("joined group.");
    // STEP 4 - construct context
    ServiceClient<CascadeTypes...>::initialize(group.get());
    context->construct();
    // STEP 5 - create service thread
    this->_is_running = true;
    service_thread = std::thread(&Service<CascadeTypes...>::run, this);
    dbg_default_trace("created daemon thread.");
}

template <typename... CascadeTypes>
Service<CascadeTypes...>::~Service() {
    dbg_default_trace("{}:{} Service destructor is called.", __FILE__, __LINE__);
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::run() {
    std::unique_lock<std::mutex> lck(this->service_control_mutex);
    this->service_control_cv.wait(lck, [this]() { return !this->_is_running; });
    // stop gracefully
    group->barrier_sync();
    group->leave();
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::stop(bool is_joining) {
    std::unique_lock<std::mutex> lck(this->service_control_mutex);
    this->_is_running = false;
    lck.unlock();
    this->service_control_cv.notify_one();
    // wait until stopped.
    if(is_joining && this->service_thread.joinable()) {
        this->service_thread.join();
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::join() {
    if(this->service_thread.joinable()) {
        this->service_thread.join();
    }
}

template <typename... CascadeTypes>
bool Service<CascadeTypes...>::is_running() {
    std::lock_guard<std::mutex> lck(this->service_control_mutex);
    return _is_running;
}

#ifndef __WITHOUT_SERVICE_SINGLETONS__
template <typename... CascadeTypes>
std::unique_ptr<Service<CascadeTypes...>> Service<CascadeTypes...>::service_ptr;

template <typename... CascadeTypes>
void Service<CascadeTypes...>::start(const std::vector<DeserializationContext*>& dsms,
                                     derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_factory,
                                     derecho::cascade::Factory<CascadeTypes>... factories) {
    if(!service_ptr) {
        service_ptr = std::unique_ptr<Service<CascadeTypes...>>(new Service<CascadeTypes...>(dsms, metadata_factory, factories...));
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::shutdown(bool is_joining) {
    if(service_ptr) {
        if(service_ptr->is_running()) {
            service_ptr->stop(is_joining);
        }
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::wait() {
    if(service_ptr) {
        service_ptr->join();
    }
    service_ptr.reset();
}
#endif  //__WITHOUT_SERVICE_SINGLETONS__

template <typename CascadeType>
std::unique_ptr<CascadeType> client_stub_factory() {
    return std::make_unique<CascadeType>();
}

#ifdef ENABLE_EVALUATION
#define LOG_SERVICE_CLIENT_TIMESTAMP(tag, msgid) \
    TimestampLogger::log(tag, this->get_my_id(), msgid, get_walltime());
#else
#define LOG_SERVICE_CLIENT_TIMESTAMP(tag, msgid)
#endif

template <typename... CascadeTypes>
ServiceClient<CascadeTypes...>::ServiceClient(derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* _group_ptr) : external_group_ptr(nullptr),
                                                                                                                                      group_ptr(_group_ptr) {
    if(group_ptr == nullptr) {
        this->external_group_ptr = std::make_unique<derecho::ExternalGroupClient<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>>(
                client_stub_factory<CascadeMetadataService<CascadeTypes...>>,
                client_stub_factory<CascadeTypes>...);
    }
}

template <typename... CascadeTypes>
bool ServiceClient<CascadeTypes...>::is_external_client() const {
    return (group_ptr == nullptr) && (external_group_ptr != nullptr);
}

template <typename... CascadeTypes>
node_id_t ServiceClient<CascadeTypes...>::get_my_id() const {
    if(!is_external_client()) {
        return group_ptr->get_my_id();
    } else {
        return external_group_ptr->get_my_id();
    }
}

template <typename... CascadeTypes>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_members() const {
    if(!is_external_client()) {
        return group_ptr->get_members();
    } else {
        return external_group_ptr->get_members();
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_shard_members(uint32_t subgroup_index, uint32_t shard_index) const {
    if(!is_external_client()) {
        std::vector<std::vector<node_id_t>> subgroup_members = group_ptr->template get_subgroup_members<SubgroupType>(subgroup_index);
        if(subgroup_members.size() > shard_index) {
            return subgroup_members[shard_index];
        } else {
            return {};
        }
    } else {
        return external_group_ptr->template get_shard_members<SubgroupType>(subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::type_recursive_get_shard_members(uint32_t type_index,
                                                                                        uint32_t subgroup_index, uint32_t shard_index) const {
    if(type_index == 0) {
        return this->template get_shard_members<FirstType>(subgroup_index, shard_index);
    } else {
        return this->template type_recursive_get_shard_members<SecondType, RestTypes...>(type_index - 1, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::type_recursive_get_shard_members(uint32_t type_index,
                                                                                        uint32_t subgroup_index, uint32_t shard_index) const {
    if(type_index == 0) {
        return this->template get_shard_members<LastType>(subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + " type index is out of boundary");
    }
}

template <typename... CascadeTypes>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_shard_members(
        const std::string& object_pool_pathname, uint32_t shard_index) {
    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }
    return this->template type_recursive_get_shard_members<CascadeTypes...>(opm.subgroup_type_index, opm.subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<std::vector<node_id_t>> ServiceClient<CascadeTypes...>::get_subgroup_members(uint32_t subgroup_index) const {
    if(!is_external_client()) {
        return group_ptr->template get_subgroup_members<SubgroupType>(subgroup_index);
    } else {
        return external_group_ptr->template get_subgroup_members<SubgroupType>(subgroup_index);
    }
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
std::vector<std::vector<node_id_t>> ServiceClient<CascadeTypes...>::type_recursive_get_subgroup_members(
        uint32_t type_index, uint32_t subgroup_index) const {
    if(type_index == 0) {
        return this->template get_subgroup_members<FirstType>(subgroup_index);
    } else {
        return this->template type_recursive_get_subgroup_members<SecondType, RestTypes...>(type_index - 1, subgroup_index);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
std::vector<std::vector<node_id_t>> ServiceClient<CascadeTypes...>::type_recursive_get_subgroup_members(
        uint32_t type_index, uint32_t subgroup_index) const {
    if(type_index == 0) {
        return this->template get_subgroup_members<LastType>(subgroup_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + " type index is out of boundary");
    }
}

template <typename... CascadeTypes>
std::vector<std::vector<node_id_t>> ServiceClient<CascadeTypes...>::get_subgroup_members(
        const std::string& object_pool_pathname) {
    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }
    return this->template type_recursive_get_subgroup_members<CascadeTypes...>(opm.subgroup_type_index, opm.subgroup_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_subgroups() const {
    if(!is_external_client()) {
        return group_ptr->template get_num_subgroups<SubgroupType>();
    } else {
        return external_group_ptr->template get_number_of_subgroups<SubgroupType>();
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_shards(uint32_t subgroup_index) const {
    if(!is_external_client()) {
        return group_ptr->template get_subgroup_members<SubgroupType>(subgroup_index).size();
    } else {
        return external_group_ptr->template get_number_of_shards<SubgroupType>(subgroup_index);
    }
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
uint32_t ServiceClient<CascadeTypes...>::type_recursive_get_number_of_shards(
        uint32_t type_index, uint32_t subgroup_index) const {
    if(type_index == 0) {
        return this->template get_number_of_shards<FirstType>(subgroup_index);
    } else {
        return this->template type_recursive_get_number_of_shards<SecondType, RestTypes...>(type_index - 1, subgroup_index);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
uint32_t ServiceClient<CascadeTypes...>::type_recursive_get_number_of_shards(
        uint32_t type_index, uint32_t subgroup_index) const {
    if(type_index == 0) {
        return this->template get_number_of_shards<LastType>(subgroup_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + " type index is out of boundary");
    }
}

template <typename... CascadeTypes>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_shards(
        uint32_t subgroup_type_index, uint32_t subgroup_index) const {
    return type_recursive_get_number_of_shards<CascadeTypes...>(subgroup_type_index, subgroup_index);
}

template <typename... CascadeTypes>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_shards(
        const std::string& object_pool_pathname) {
    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }
    return get_number_of_shards(opm.subgroup_type_index, opm.subgroup_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::set_member_selection_policy(uint32_t subgroup_index, uint32_t shard_index,
                                                                 ShardMemberSelectionPolicy policy, node_id_t user_specified_node_id) {
    // write lock policies
    std::unique_lock wlck(this->member_selection_policies_mutex);
    // update map
    this->member_selection_policies[std::make_tuple(std::type_index(typeid(SubgroupType)), subgroup_index, shard_index)] = std::make_tuple(policy, user_specified_node_id);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::tuple<ShardMemberSelectionPolicy, node_id_t> ServiceClient<CascadeTypes...>::get_member_selection_policy(
        uint32_t subgroup_index, uint32_t shard_index) const {
    // read lock policies
    std::shared_lock rlck(this->member_selection_policies_mutex);
    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)), subgroup_index, shard_index);
    // read map
    if(member_selection_policies.find(key) != member_selection_policies.end()) {
        return member_selection_policies.at(key);
    } else {
        return std::make_tuple(DEFAULT_SHARD_MEMBER_SELECTION_POLICY, INVALID_NODE_ID);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::refresh_member_cache_entry(uint32_t subgroup_index,
                                                                uint32_t shard_index) {
    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)), subgroup_index, shard_index);
    auto members = get_shard_members<SubgroupType>(subgroup_index, shard_index);
    std::shared_lock rlck(member_cache_mutex);
    if(member_cache.find(key) == member_cache.end()) {
        rlck.unlock();
        std::unique_lock wlck(member_cache_mutex);
        member_cache[key] = members;
    } else {
        member_cache[key].swap(members);
    }
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
void ServiceClient<CascadeTypes...>::type_recursive_refresh_member_cache() {
    auto num_subgroups =  get_number_of_subgroups<FirstType>();
    for(uint32_t subgroup_index = 0; subgroup_index < num_subgroups; ++subgroup_index) {
        auto num_shards = get_number_of_shards<FirstType>(subgroup_index);
        for(uint32_t shard_index = 0; shard_index < num_shards; ++shard_index) {
            refresh_member_cache_entry<FirstType>(subgroup_index, shard_index);
        }
    }
    type_recursive_refresh_member_cache<SecondType, RestTypes...>();   
}

template <typename... CascadeTypes>
template <typename LastType>
void ServiceClient<CascadeTypes...>::type_recursive_refresh_member_cache() {
    auto num_subgroups = get_number_of_subgroups<LastType>();
    for(uint32_t subgroup_index = 0; subgroup_index < num_subgroups; ++subgroup_index) {
        auto num_shards = get_number_of_shards<LastType>(subgroup_index);
        for(uint32_t shard_index = 0; shard_index < num_shards; ++shard_index) {
            refresh_member_cache_entry<LastType>(subgroup_index, shard_index);
        }
    }
}

template <typename... CascadeTypes>
template <typename KeyType>
std::tuple<uint32_t, uint32_t, uint32_t> ServiceClient<CascadeTypes...>::key_to_shard(
        const KeyType& key,
        bool check_object_location) {
    std::string object_pool_pathname = get_pathname<KeyType>(key);
    if(object_pool_pathname.empty()) {
        std::string exp_msg("Key:");
        throw derecho::derecho_exception(std::string("Key:") + key + " does not belong to any object pool.");
    }

    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }
    return std::tuple<uint32_t, uint32_t, uint32_t>{opm.subgroup_type_index, opm.subgroup_index,
                                                    opm.key_to_shard_index(key, get_number_of_shards(opm.subgroup_type_index, opm.subgroup_index), check_object_location)};
}

template <typename... CascadeTypes>
std::tuple<std::type_index, uint32_t> ServiceClient<CascadeTypes...>::node_id_to_subgroup(node_id_t node_id){
    std::shared_lock rlck(member_cache_mutex);
    for(auto& entry : member_cache) {
        for(auto& member : entry.second) {
            if(member == node_id) {
                return std::make_tuple(std::get<0>(entry.first), std::get<1>(entry.first));
            }
        }
    }
    throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": node_id(" + std::to_string(node_id) + ") does not exist in any subgroups");
}

template <typename... CascadeTypes>
template <typename SubgroupType, typename KeyTypeForHashing>
node_id_t ServiceClient<CascadeTypes...>::pick_member_by_policy(uint32_t subgroup_index,
                                                                uint32_t shard_index,
                                                                const KeyTypeForHashing& key_for_hashing,
                                                                bool retry) {
    ShardMemberSelectionPolicy policy;
    node_id_t last_specified_node_id_or_index;

    std::tie(policy, last_specified_node_id_or_index) = get_member_selection_policy<SubgroupType>(subgroup_index, shard_index);

    if(policy == ShardMemberSelectionPolicy::UserSpecified) {
        return last_specified_node_id_or_index;
    }

    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)), subgroup_index, shard_index);

    if(member_cache.find(key) == member_cache.end() || retry) {
        refresh_member_cache_entry<SubgroupType>(subgroup_index, shard_index);
    }

    std::shared_lock rlck(member_cache_mutex);

    node_id_t node_id = last_specified_node_id_or_index;

    switch(policy) {
        case ShardMemberSelectionPolicy::FirstMember:
            node_id = member_cache.at(key).front();
            break;
        case ShardMemberSelectionPolicy::LastMember:
            node_id = member_cache.at(key).back();
            break;
        case ShardMemberSelectionPolicy::Random:
            node_id = member_cache.at(key)[get_time() % member_cache.at(key).size()];  // use time as random source.
            break;
        case ShardMemberSelectionPolicy::FixedRandom:
            if(node_id == INVALID_NODE_ID || retry) {
                node_id = member_cache.at(key)[get_time() % member_cache.at(key).size()];  // use time as random source.
            }
            break;
        case ShardMemberSelectionPolicy::RoundRobin: {
            std::shared_lock rlck(member_selection_policies_mutex);
            node_id = static_cast<uint32_t>(node_id + 1) % member_cache.at(key).size();
            auto new_policy = std::make_tuple(ShardMemberSelectionPolicy::RoundRobin, node_id);
            member_selection_policies[key].swap(new_policy);
        }
            node_id = member_cache.at(key)[node_id];
            break;
        case ShardMemberSelectionPolicy::KeyHashing: {
            uint64_t hash = 0;
            if constexpr(std::is_integral_v<KeyTypeForHashing>) {
                hash = static_cast<uint64_t>(key_for_hashing);
            } else if constexpr(std::is_convertible_v<KeyTypeForHashing, std::string>) {
                hash = std::hash<std::string>{}(std::string(key_for_hashing));
            } else {
                dbg_default_warn("Key type {} is neither integral nor string, falling back to FirstMember policy. {}:{}",
                                 typeid(KeyTypeForHashing).name(), __FILE__, __LINE__);
            }
            node_id = member_cache.at(key)[hash % member_cache.at(key).size()];
        } break;
        default:
            throw derecho::derecho_exception("Unknown member selection policy:" + std::to_string(static_cast<unsigned int>(policy)));
    }

    return node_id;
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::put(
        const typename SubgroupType::ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_PUT_START,
                                 (std::is_base_of<IHasMessageID, typename SubgroupType::ObjectType>::value ? value.get_message_id() : 0));
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // ordered put as a shard member
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
        } else {
            // p2p put
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, value.get_key_ref());
            try {
                // as a subgroup member
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                return subgroup_handle.template p2p_send<RPC_NAME(put)>(node_id, value);
            } catch(derecho::invalid_subgroup_exception& ex) {
                // as an external caller
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                return subgroup_handle.template p2p_send<RPC_NAME(put)>(node_id, value);
            }
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, value.get_key_ref());
        return caller.template p2p_send<RPC_NAME(put)>(node_id, value);
    }
}

template <typename... CascadeTypes>
template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::type_recursive_put(
        uint32_t type_index,
        const ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template put<FirstType>(value, subgroup_index, shard_index);
    } else {
        return this->template type_recursive_put<ObjectType, SecondType, RestTypes...>(type_index - 1, value, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename ObjectType, typename LastType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::type_recursive_put(
        uint32_t type_index,
        const ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template put<LastType>(value, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename ObjectType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::put(
        const ObjectType& value) {
    // STEP 1 - get key
    if constexpr(!std::is_base_of_v<ICascadeObject<std::string, ObjectType>, ObjectType>) {
        throw derecho::derecho_exception(std::string("ServiceClient<>::put() only support object of type ICascadeObject<std::string,ObjectType>,but we get ") + typeid(ObjectType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(value.get_key_ref());

    // STEP 3 - call recursive put
    return this->template type_recursive_put<ObjectType, CascadeTypes...>(subgroup_type_index, value, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::put_and_forget(
        const typename SubgroupType::ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_PUT_AND_FORGET_START,
                                 (std::is_base_of<IHasMessageID, typename SubgroupType::ObjectType>::value ? value.get_message_id() : 0));
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered put as a shard member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value);
        } else {
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, value.get_key_ref());
            // do p2p put
            try {
                // as a subgroup member
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                subgroup_handle.template p2p_send<RPC_NAME(put_and_forget)>(node_id, value);
            } catch(derecho::invalid_subgroup_exception& ex) {
                // as an external caller
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                subgroup_handle.template p2p_send<RPC_NAME(put_and_forget)>(node_id, value);
            }
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, value.get_key_ref());
        caller.template p2p_send<RPC_NAME(put_and_forget)>(node_id, value);
    }
}

template <typename... CascadeTypes>
template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
void ServiceClient<CascadeTypes...>::type_recursive_put_and_forget(
        uint32_t type_index,
        const ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        put_and_forget<FirstType>(value, subgroup_index, shard_index);
    } else {
        type_recursive_put_and_forget<ObjectType, SecondType, RestTypes...>(type_index - 1, value, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename ObjectType, typename LastType>
void ServiceClient<CascadeTypes...>::type_recursive_put_and_forget(
        uint32_t type_index,
        const ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        put_and_forget<LastType>(value, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename ObjectType>
void ServiceClient<CascadeTypes...>::put_and_forget(const ObjectType& value) {
    // STEP 1 - get key
    if constexpr(!std::is_base_of_v<ICascadeObject<std::string, ObjectType>, ObjectType>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports object of type ICascadeObject<std::string,ObjectType>,but we get ") + typeid(ObjectType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(value.get_key_ref());

    // STEP 3 - call recursive put_and_forget
    this->template type_recursive_put_and_forget<ObjectType, CascadeTypes...>(subgroup_type_index, value, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::trigger_put(
        const typename SubgroupType::ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_TRIGGER_PUT_START,
                                 (std::is_base_of<IHasMessageID, typename SubgroupType::ObjectType>::value ? value.get_message_id() : 0));
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, value.get_key_ref());
            dbg_default_trace("trigger_put to node {}", node_id);
            return subgroup_handle.template p2p_send<RPC_NAME(trigger_put)>(node_id, value);
        } else {
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, value.get_key_ref());
            dbg_default_trace("trigger_put to node {}", node_id);
            return subgroup_handle.template p2p_send<RPC_NAME(trigger_put)>(node_id, value);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, value.get_key_ref());
        dbg_default_trace("trigger_put to node {}", node_id);
        return caller.template p2p_send<RPC_NAME(trigger_put)>(node_id, value);
    }
}

template <typename... CascadeTypes>
template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
void ServiceClient<CascadeTypes...>::type_recursive_single_node_trigger_put(
        std::type_index type_index,
        const ObjectType& value,
        uint32_t subgroup_index,
        node_id_t node_id,
        uint32_t round) {
    if(type_index == std::type_index(typeid(FirstType))) {
        dbg_default_trace("Before put to round{}", round);
        single_node_trigger_put<FirstType>(value, subgroup_index, node_id);
    } else {
        type_recursive_single_node_trigger_put<ObjectType, SecondType, RestTypes...>(type_index, value, subgroup_index, node_id, round++);
    }
}

template <typename... CascadeTypes>
template <typename ObjectType, typename LastType>
void ServiceClient<CascadeTypes...>::type_recursive_single_node_trigger_put(
        std::type_index type_index,
        const ObjectType& value,
        uint32_t subgroup_index,
        node_id_t node_id,
        uint32_t round) {
    if(type_index == std::type_index(typeid(LastType))) { 
        dbg_default_trace("Before put to round{}", round);
        single_node_trigger_put<LastType>(value, subgroup_index, node_id);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::single_node_trigger_put(
        const typename SubgroupType::ObjectType& object,
        uint32_t subgroup_index,
        node_id_t node_id) {
    std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
    if(group_ptr->template get_my_shard<SubgroupType>(subgroup_index) != -1 ){
        auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
        if( node_id != group_ptr->get_my_id()) {
            subgroup_handle.template p2p_send<RPC_NAME(trigger_put)>(node_id, object);
        }else{
            /** If trigger put is used to put the object to the same node by scheduler, then directly call trigger_put function on this server
             * instead of calling it as an external client. This design is for scheduler performance consideration, but coud be adjust if the other way
             * is better in preserving the modularity of the program. */
            subgroup_handle.get_ref().trigger_put(object);
        }
    } else {
        if( node_id == group_ptr->get_my_id()) {
            dbg_default_trace("!!!OH NOOO! not the same subgroup but same node_id!!!!");
        }
        auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
        subgroup_handle.template p2p_send<RPC_NAME(trigger_put)>(node_id, object);
    }
}

template <typename... CascadeTypes>
template <typename ObjectType>
void ServiceClient<CascadeTypes...>::single_node_trigger_put(
        const ObjectType object,
        node_id_t node_id) {
    try{
        this->template type_recursive_refresh_member_cache<CascadeTypes...>();
        auto subgroup_info = node_id_to_subgroup(node_id);
        std::type_index& type_index = std::get<0>(subgroup_info);
        uint32_t& subgroup_index = std::get<1>(subgroup_info);
        this->template type_recursive_single_node_trigger_put<ObjectType, CascadeTypes...>(type_index, object, subgroup_index, node_id, 0);
    } catch(derecho::derecho_exception& ex) {
        dbg_default_warn("Invalid subgroup exception: {}", ex.what());
    }
}

template <typename... CascadeTypes>
template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::type_recursive_trigger_put(
        uint32_t type_index,
        const ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return trigger_put<FirstType>(value, subgroup_index, shard_index);
    } else {
        return type_recursive_trigger_put<ObjectType, SecondType, RestTypes...>(type_index - 1, value, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename ObjectType, typename LastType>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::type_recursive_trigger_put(
        uint32_t type_index,
        const ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return trigger_put<LastType>(value, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename ObjectType>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::trigger_put(
        const ObjectType& value) {
    // STEP 1 - get key
    if constexpr(!std::is_base_of_v<ICascadeObject<std::string, ObjectType>, ObjectType>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports object of type ICascadeObject<std::string,ObjectType>,but we get ") + typeid(ObjectType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(value.get_key_ref());

    // STEP 3 - call recursive trigger_put
    return this->template type_recursive_trigger_put<ObjectType, CascadeTypes...>(subgroup_type_index, value, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::collective_trigger_put(
        const typename SubgroupType::ObjectType& value,
        uint32_t subgroup_index,
        std::unordered_map<node_id_t, std::unique_ptr<derecho::rpc::QueryResults<void>>>& nodes_and_futures) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_COLLECTIVE_TRIGGER_PUT_START,
                                 (std::is_base_of<IHasMessageID, typename SubgroupType::ObjectType>::value ? value.get_message_id() : 0));
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        if(group_ptr->template get_my_shard<SubgroupType>(subgroup_index) != -1) {
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            for(auto& kv : nodes_and_futures) {
                nodes_and_futures[kv.first] = std::make_unique<derecho::rpc::QueryResults<void>>(
                        std::move(subgroup_handle.template p2p_send<RPC_NAME(trigger_put)>(kv.first, value)));
            }
        } else {
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            for(auto& kv : nodes_and_futures) {
                nodes_and_futures[kv.first] = std::make_unique<derecho::rpc::QueryResults<void>>(
                        std::move(subgroup_handle.template p2p_send<RPC_NAME(trigger_put)>(kv.first, value)));
            }
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        for(auto& kv : nodes_and_futures) {
            nodes_and_futures[kv.first] = std::make_unique<derecho::rpc::QueryResults<void>>(
                    std::move(caller.template p2p_send<RPC_NAME(trigger_put)>(kv.first, value)));
        }
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::remove(
        const typename SubgroupType::KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_REMOVE_START, 0);
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered remove as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
        } else {
            // do p2p remove
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
            try {
                // as a subgroup member
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                return subgroup_handle.template p2p_send<RPC_NAME(remove)>(node_id, key);
            } catch(derecho::invalid_subgroup_exception& ex) {
                // as an external caller
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                return subgroup_handle.template p2p_send<RPC_NAME(remove)>(node_id, key);
            }
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        return caller.template p2p_send<RPC_NAME(remove)>(node_id, key);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::type_recursive_remove(
        uint32_t type_index,
        const KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template remove<FirstType>(key, subgroup_index, shard_index);
    } else {
        return this->template type_recursive_remove<KeyType, SecondType, RestTypes...>(type_index - 1, key, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename LastType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::type_recursive_remove(
        uint32_t type_index,
        const KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template remove<LastType>(key, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename KeyType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::remove(
        const KeyType& key) {
    // STEP 1 - get key
    if constexpr(!std::is_convertible_v<KeyType, std::string>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports string key,but we get ") + typeid(KeyType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(key);

    // STEP 3 - call recursive remove
    return this->template type_recursive_remove<KeyType, CascadeTypes...>(subgroup_type_index, key, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::get(
        const typename SubgroupType::KeyType& key,
        const persistent::version_t& version,
        bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_GET_START, 0);
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        try {
            // do p2p get as a subgroup member
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                node_id = group_ptr->get_my_id();
                // local get
                auto obj = subgroup_handle.get_ref().get(key, version, stable);
                auto pending_results = std::make_shared<PendingResults<const typename SubgroupType::ObjectType>>();
                pending_results->fulfill_map({node_id});
                pending_results->set_value(node_id, obj);
                auto query_results = pending_results->get_future();
                return std::move(*query_results);
            }
            return subgroup_handle.template p2p_send<RPC_NAME(get)>(node_id, key, version, stable, false);
        } catch(derecho::invalid_subgroup_exception& ex) {
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get)>(node_id, key, version, stable, false);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        return caller.template p2p_send<RPC_NAME(get)>(node_id, key, version, stable, false);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::multi_get(
        const typename SubgroupType::KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_MULTI_GET_START, 0);
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        try {
            // do p2p multi_get as a subgroup member.
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                node_id = group_ptr->get_my_id();
            }
            return subgroup_handle.template p2p_send<RPC_NAME(multi_get)>(node_id, key);
        } catch(derecho::invalid_subgroup_exception& ex) {
            // do p2p multi_get as an external caller.
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(multi_get)>(node_id, key);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        return caller.template p2p_send<RPC_NAME(multi_get)>(node_id, key);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
auto ServiceClient<CascadeTypes...>::type_recursive_get(
        uint32_t type_index,
        const KeyType& key,
        const persistent::version_t& version,
        bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template get<FirstType>(key, version, stable, subgroup_index, shard_index);
    } else {
        return this->template type_recursive_get<KeyType, SecondType, RestTypes...>(type_index - 1, key, version, stable, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename LastType>
auto ServiceClient<CascadeTypes...>::type_recursive_get(
        uint32_t type_index,
        const KeyType& key,
        const persistent::version_t& version,
        bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template get<LastType>(key, version, stable, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename KeyType>
auto ServiceClient<CascadeTypes...>::get(
        // const std::decay_t<typename std::result_of_t<decltype(&ObjectType::get_key_ref())>>& key,
        const KeyType& key,
        const persistent::version_t& version,
        bool stable) {
    // STEP 1 - get key
    if constexpr(!std::is_convertible_v<KeyType, std::string>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports string key,but we get ") + typeid(KeyType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(key);

    // STEP 3 - call recursive get
    return this->template type_recursive_get<KeyType, CascadeTypes...>(subgroup_type_index, key, version, stable, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
auto ServiceClient<CascadeTypes...>::type_recursive_multi_get(
        uint32_t type_index,
        const KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template multi_get<FirstType>(key, subgroup_index, shard_index);
    } else {
        return this->template type_recursive_multi_get<KeyType, SecondType, RestTypes...>(type_index - 1, key, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename LastType>
auto ServiceClient<CascadeTypes...>::type_recursive_multi_get(
        uint32_t type_index,
        const KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template multi_get<LastType>(key, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename KeyType>
auto ServiceClient<CascadeTypes...>::multi_get(
        const KeyType& key) {
    // STEP 1 - get key
    if constexpr(!std::is_convertible_v<KeyType, std::string>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports string key,but we get ") + typeid(KeyType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(key);

    // STEP 3 - call recursive get
    return this->template type_recursive_multi_get<KeyType, CascadeTypes...>(subgroup_type_index, key, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::get_by_time(
        const typename SubgroupType::KeyType& key,
        const uint64_t& ts_us,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        try {
            // do p2p get_by_time
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                // as a shard member.
                node_id = group_ptr->get_my_id();
            }
            return subgroup_handle.template p2p_send<RPC_NAME(get_by_time)>(node_id, key, ts_us, stable);
        } catch(derecho::invalid_subgroup_exception& ex) {
            // do p2p get_by_time as an external caller
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_by_time)>(node_id, key, ts_us, stable);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>();
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        return caller.template p2p_send<RPC_NAME(get_by_time)>(node_id, key, ts_us, stable);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
auto ServiceClient<CascadeTypes...>::type_recursive_get_by_time(
        uint32_t type_index,
        const KeyType& key,
        const uint64_t& ts_us,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template get_by_time<FirstType>(key, ts_us, stable, subgroup_index, shard_index);
    } else {
        return this->template type_recursive_get_by_time<KeyType, SecondType, RestTypes...>(type_index - 1, key, ts_us, stable, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename LastType>
auto ServiceClient<CascadeTypes...>::type_recursive_get_by_time(
        uint32_t type_index,
        const KeyType& key,
        const uint64_t& ts_us,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template get_by_time<LastType>(key, ts_us, stable, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename KeyType>
auto ServiceClient<CascadeTypes...>::get_by_time(
        const KeyType& key,
        const uint64_t& ts_us,
        const bool stable) {
    // STEP 1 - get key
    if constexpr(!std::is_convertible_v<KeyType, std::string>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports string key,but we get ") + typeid(KeyType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(key);

    // STEP 3 - call recursive get_by_time
    return this->template type_recursive_get_by_time<KeyType, CascadeTypes...>(subgroup_type_index, key, ts_us, stable, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size(
        const typename SubgroupType::KeyType& key,
        const persistent::version_t& version,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_GET_SIZE_START, 0);
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        try {
            // do p2p get_size as a subgroup_member
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                // as a shard member.
                node_id = group_ptr->get_my_id();
            }
            return subgroup_handle.template p2p_send<RPC_NAME(get_size)>(node_id, key, version, stable, false);
        } catch(derecho::invalid_subgroup_exception& ex) {
            // do p2p get_size as an external caller
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_size)>(node_id, key, version, stable, false);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        return caller.template p2p_send<RPC_NAME(get_size)>(node_id, key, version, stable, false);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::type_recursive_get_size(
        uint32_t type_index,
        const KeyType& key,
        const persistent::version_t& version,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template get_size<FirstType>(key, version, stable, subgroup_index, shard_index);
    } else {
        return this->template type_recursive_get_size<KeyType, SecondType, RestTypes...>(type_index - 1, key, version, stable, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename LastType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::type_recursive_get_size(
        uint32_t type_index,
        const KeyType& key,
        const persistent::version_t& version,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template get_size<LastType>(key, version, stable, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename KeyType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size(
        const KeyType& key,
        const persistent::version_t& version,
        const bool stable) {
    // STEP 1 - verify the keys
    if constexpr(!std::is_convertible_v<KeyType, std::string>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports string key,but we get ") + typeid(KeyType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(key);

    // STEP 3 - call recursive get
    return this->template type_recursive_get_size<KeyType, CascadeTypes...>(subgroup_type_index, key, version, stable, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::multi_get_size(
        const typename SubgroupType::KeyType& key,
        uint32_t subgroup_index, uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_MULTI_GET_SIZE_START, 0);
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        try {
            // do p2p multi_get_size as a subgroup member.
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                node_id = group_ptr->get_my_id();
            }
            return subgroup_handle.template p2p_send<RPC_NAME(multi_get_size)>(node_id, key);
        } catch(derecho::invalid_subgroup_exception& ex) {
            // do p2p multi_get_size as an external caller.
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(multi_get_size)>(node_id, key);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        return caller.template p2p_send<RPC_NAME(multi_get_size)>(node_id, key);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::type_recursive_multi_get_size(
        uint32_t type_index,
        const KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template multi_get_size<FirstType>(key, subgroup_index, shard_index);
    } else {
        return this->template type_recursive_multi_get_size<KeyType, SecondType, RestTypes...>(type_index - 1, key, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename LastType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::type_recursive_multi_get_size(
        uint32_t type_index,
        const KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template multi_get_size<LastType>(key, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename KeyType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::multi_get_size(const KeyType& key) {
    // STEP 1 - verify the keys
    if constexpr(!std::is_convertible_v<KeyType, std::string>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports string key,but we get ") + typeid(KeyType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(key);

    // STEP 3 - call recursive get
    return this->template type_recursive_multi_get_size<KeyType, CascadeTypes...>(subgroup_type_index, key, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size_by_time(
        const typename SubgroupType::KeyType& key,
        const uint64_t& ts_us,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        try {
            // do p2p get_size_by_time as a subgroup member.
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                node_id = group_ptr->get_my_id();
            }
            return subgroup_handle.template p2p_send<RPC_NAME(get_size_by_time)>(node_id, key, ts_us, stable);
        } catch(derecho::invalid_subgroup_exception& ex) {
            // do p2p get_size_by_time as an external caller.
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_size_by_time)>(node_id, key, ts_us, stable);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, key);
        return caller.template p2p_send<RPC_NAME(get_size_by_time)>(node_id, key, ts_us, stable);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::type_recursive_get_size_by_time(
        uint32_t type_index,
        const KeyType& key,
        const uint64_t& ts_us,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template get_size_by_time<FirstType>(key, ts_us, stable, subgroup_index, shard_index);
    } else {
        return this->template type_recursive_get_size_by_time<KeyType, SecondType, RestTypes...>(type_index - 1, key, ts_us, stable, subgroup_index, shard_index);
    }
}

template <typename... CascadeTypes>
template <typename KeyType, typename LastType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::type_recursive_get_size_by_time(
        uint32_t type_index,
        const KeyType& key,
        const uint64_t& ts_us,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(type_index == 0) {
        return this->template get_size_by_time<LastType>(key, ts_us, stable, subgroup_index, shard_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename KeyType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size_by_time(
        const KeyType& key,
        const uint64_t& ts_us,
        const bool stable) {
    // STEP 1 - verify the keys
    if constexpr(!std::is_convertible_v<KeyType, std::string>) {
        throw derecho::derecho_exception(__PRETTY_FUNCTION__ + std::string(" only supports string key,but we get ") + typeid(KeyType).name());
    }

    // STEP 2 - get shard
    uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(key);

    // STEP 3 - call recursive get
    return this->template type_recursive_get_size_by_time<KeyType, CascadeTypes...>(subgroup_type_index, key, ts_us, stable, subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> ServiceClient<CascadeTypes...>::list_keys(
        const persistent::version_t& version,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_LIST_KEYS_START, 0);
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
        try {
            // do p2p list_keys as a subgroup member.
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                node_id = group_ptr->get_my_id();
            }
            return subgroup_handle.template p2p_send<RPC_NAME(list_keys)>(node_id, "", version, stable);
        } catch(derecho::invalid_subgroup_exception& ex) {
            // do p2p list_keys as an external client.
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(list_keys)>(node_id, "", version, stable);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
        return caller.template p2p_send<RPC_NAME(list_keys)>(node_id, "", version, stable);
    }
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
auto ServiceClient<CascadeTypes...>::type_recursive_list_keys(
        uint32_t type_index,
        const persistent::version_t& version,
        const bool stable,
        const std::string& object_pool_pathname) {
    if(type_index == 0) {
        return this->template __list_keys<FirstType>(version, stable, object_pool_pathname);
    } else {
        return this->template type_recursive_list_keys<SecondType, RestTypes...>(type_index - 1, version, stable, object_pool_pathname);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
auto ServiceClient<CascadeTypes...>::type_recursive_list_keys(
        uint32_t type_index,
        const persistent::version_t& version,
        const bool stable,
        const std::string& object_pool_pathname) {
    if(type_index == 0) {
        return this->template __list_keys<LastType>(version, stable, object_pool_pathname);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> ServiceClient<CascadeTypes...>::__list_keys(
        const persistent::version_t& version,
        const bool stable,
        const std::string& object_pool_pathname) {
    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }
    uint32_t subgroup_index = opm.subgroup_index;
    uint32_t shards = get_number_of_shards<SubgroupType>(subgroup_index);
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> result;
    for(uint32_t shard_index = 0; shard_index < shards; shard_index++) {
        if(!is_external_client()) {
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
            try {
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                    node_id = group_ptr->get_my_id();
                }
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(list_keys)>(node_id, object_pool_pathname, version, stable);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            } catch(derecho::invalid_subgroup_exception& ex) {
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(list_keys)>(node_id, object_pool_pathname, version, stable);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            }
        } else {
            std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
            auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
            auto shard_keys = caller.template p2p_send<RPC_NAME(list_keys)>(node_id, object_pool_pathname, version, stable);
            result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
        }
    }
    return result;
}

template <typename... CascadeTypes>
auto ServiceClient<CascadeTypes...>::list_keys(
        const persistent::version_t& version,
        const bool stable,
        const std::string& object_pool_pathname) {
    volatile uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(object_pool_pathname + "/_");
    return this->template type_recursive_list_keys<CascadeTypes...>(subgroup_type_index, version, stable, object_pool_pathname);
}

template <typename ReturnType>
inline ReturnType wait_for_future(derecho::rpc::QueryResults<ReturnType>& result) {
    // iterate through ReplyMap
    for(auto& reply_future : result.get()) {
        ReturnType reply = static_cast<ReturnType>(reply_future.second.get());
        return reply;
    }
    return ReturnType();
}

template <typename... CascadeTypes>
template <typename KeyType>
std::vector<KeyType> ServiceClient<CascadeTypes...>::wait_list_keys(
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<KeyType>>>>& future) {
    std::vector<KeyType> result;
    // iterate over each shard's Query result
    for(auto& query_result : future) {
        std::vector<KeyType> reply = wait_for_future<std::vector<KeyType>>(*(query_result.get()));
        std::move(reply.begin(), reply.end(), std::back_inserter(result));
    }
    return result;
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> ServiceClient<CascadeTypes...>::multi_list_keys(
        uint32_t subgroup_index,
        uint32_t shard_index) {
    LOG_SERVICE_CLIENT_TIMESTAMP(TLT_SERVICE_CLIENT_MULTI_LIST_KEYS_START, 0);
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
        try {
            // do p2p multi_list_keys as a subgroup member.
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                node_id = group_ptr->get_my_id();
            }
            return subgroup_handle.template p2p_send<RPC_NAME(multi_list_keys)>(node_id, "");
        } catch(derecho::invalid_subgroup_exception& ex) {
            // do p2p multi_list_keys as an external client.
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
            return subgroup_handle.template p2p_send<RPC_NAME(multi_list_keys)>(node_id, "");
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
        return caller.template p2p_send<RPC_NAME(multi_list_keys)>(node_id, "");
    }
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
auto ServiceClient<CascadeTypes...>::type_recursive_multi_list_keys(
        uint32_t type_index,
        const std::string& object_pool_pathname) {
    if(type_index == 0) {
        return this->template __multi_list_keys<FirstType>(object_pool_pathname);
    } else {
        return this->template type_recursive_multi_list_keys<SecondType, RestTypes...>(type_index - 1, object_pool_pathname);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
auto ServiceClient<CascadeTypes...>::type_recursive_multi_list_keys(
        uint32_t type_index,
        const std::string& object_pool_pathname) {
    if(type_index == 0) {
        return this->template __multi_list_keys<LastType>(object_pool_pathname);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> ServiceClient<CascadeTypes...>::__multi_list_keys(const std::string& object_pool_pathname) {
    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }
    uint32_t subgroup_index = opm.subgroup_index;
    uint32_t shards = get_number_of_shards<SubgroupType>(subgroup_index);
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> result;
    for(uint32_t shard_index = 0; shard_index < shards; shard_index++) {
        if(!is_external_client()) {
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, object_pool_pathname);
            try {
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                    node_id = group_ptr->get_my_id();
                }
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(multi_list_keys)>(node_id, object_pool_pathname);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            } catch(derecho::invalid_subgroup_exception& ex) {
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(multi_list_keys)>(node_id, object_pool_pathname);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            }
        } else {
            std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
            auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, object_pool_pathname);
            auto shard_keys = caller.template p2p_send<RPC_NAME(multi_list_keys)>(node_id, object_pool_pathname);
            result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
        }
    }
    return result;
}

template <typename... CascadeTypes>
auto ServiceClient<CascadeTypes...>::multi_list_keys(const std::string& object_pool_pathname) {
    volatile uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(object_pool_pathname + "/_");
    return this->template type_recursive_multi_list_keys<CascadeTypes...>(subgroup_type_index, object_pool_pathname);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> ServiceClient<CascadeTypes...>::list_keys_by_time(
        const uint64_t& ts_us,
        const bool stable,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
        try {
            // do p2p list_keys_by_time as a subgroup member
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                node_id = group_ptr->get_my_id();
            }
            return subgroup_handle.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id, "", ts_us, stable);
        } catch(derecho::invalid_subgroup_exception& ex) {
            // do p2p list_keys_by_time as an external client.
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id, "", ts_us, stable);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
        return caller.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id, "", ts_us, stable);
    }
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
auto ServiceClient<CascadeTypes...>::type_recursive_list_keys_by_time(
        uint32_t type_index,
        const uint64_t& ts_us,
        const bool stable,
        const std::string& object_pool_pathname) {
    if(type_index == 0) {
        return this->template __list_keys_by_time<FirstType>(ts_us, stable, object_pool_pathname);
    } else {
        return this->template type_recursive_list_keys_by_time<SecondType, RestTypes...>(type_index - 1, ts_us, stable, object_pool_pathname);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
auto ServiceClient<CascadeTypes...>::type_recursive_list_keys_by_time(
        uint32_t type_index,
        const uint64_t& ts_us,
        const bool stable,
        const std::string& object_pool_pathname) {
    if(type_index == 0) {
        return this->template __list_keys_by_time<LastType>(ts_us, stable, object_pool_pathname);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> ServiceClient<CascadeTypes...>::__list_keys_by_time(
        const uint64_t& ts_us,
        const bool stable,
        const std::string& object_pool_pathname) {
    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }
    uint32_t subgroup_index = opm.subgroup_index;
    uint32_t shards = get_number_of_shards<SubgroupType>(subgroup_index);
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> result;
    for(uint32_t shard_index = 0; shard_index < shards; shard_index++) {
        if(!is_external_client()) {
            std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, object_pool_pathname);
            try {
                // do p2p list_keys_by_time as a subgroup member.
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                    node_id = group_ptr->get_my_id();
                }
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id, object_pool_pathname, ts_us, stable);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            } catch(derecho::invalid_subgroup_exception& ex) {
                // do p2p list_keys_by_time as an external client.
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id, object_pool_pathname, ts_us, stable);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            }
        } else {
            std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
            // call as an external client (ExternalClientCaller).
            auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, object_pool_pathname);
            auto shard_keys = caller.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id, object_pool_pathname, ts_us, stable);
            result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
        }
    }
    return result;
}

template <typename... CascadeTypes>
auto ServiceClient<CascadeTypes...>::list_keys_by_time(const uint64_t& ts_us, const bool stable, const std::string& object_pool_pathname) {
    volatile uint32_t subgroup_type_index, subgroup_index, shard_index;
    std::tie(subgroup_type_index, subgroup_index, shard_index) = this->template key_to_shard(object_pool_pathname + "/_");
    return this->template type_recursive_list_keys_by_time<CascadeTypes...>(subgroup_type_index, ts_us, stable, object_pool_pathname);
}

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::refresh_object_pool_metadata_cache() {
    std::unordered_map<std::string, ObjectPoolMetadata<CascadeTypes...>> refreshed_metadata;
    uint32_t num_shards = this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);
    for(uint32_t shard = 0; shard < num_shards; shard++) {
        auto results = this->template list_keys<CascadeMetadataService<CascadeTypes...>>(CURRENT_VERSION, true, METADATA_SERVICE_SUBGROUP_INDEX, shard);
        for(auto& reply : results.get()) {         // only once
            for(auto& key : reply.second.get()) {  // iterate over keys
                // we only read the stable version.
                auto opm_result = this->template get<CascadeMetadataService<CascadeTypes...>>(key, CURRENT_VERSION, true, METADATA_SERVICE_SUBGROUP_INDEX, shard);
                for(auto& opm_reply : opm_result.get()) {  // only once
                    refreshed_metadata[key] = opm_reply.second.get();
                    break;
                }
            }
            break;
        }
    }

    std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
    this->object_pool_metadata_cache = refreshed_metadata;
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::create_object_pool(
        const std::string& pathname, const uint32_t subgroup_index,
        const sharding_policy_t sharding_policy, const std::unordered_map<std::string, uint32_t>& object_locations) {
    uint32_t subgroup_type_index = ObjectPoolMetadata<CascadeTypes...>::template get_subgroup_type_index<SubgroupType>();
    if(subgroup_type_index == ObjectPoolMetadata<CascadeTypes...>::invalid_subgroup_type_index) {
        dbg_default_crit("Create object pool failed because of invalid SubgroupType:{}", typeid(SubgroupType).name());
        throw derecho::derecho_exception(std::string("Create object pool failed because SubgroupType is invalid:") + typeid(SubgroupType).name());
    }
    ObjectPoolMetadata<CascadeTypes...> opm(pathname, subgroup_type_index, subgroup_index, sharding_policy, object_locations, false);
    // clear local cache entry.
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);
    if(object_pool_metadata_cache.find(pathname) == object_pool_metadata_cache.end()) {
        rlck.unlock();
    } else {
        rlck.unlock();
        std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
        object_pool_metadata_cache.erase(pathname);
    }
    // determine the shard index by hashing
    uint32_t metadata_service_shard_index = std::hash<std::string>{}(pathname) % this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);

    return this->template put<CascadeMetadataService<CascadeTypes...>>(opm, METADATA_SERVICE_SUBGROUP_INDEX, metadata_service_shard_index);
}

template <typename... CascadeTypes>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::remove_object_pool(const std::string& pathname) {
    // determine the shard index by hashing
    uint32_t metadata_service_shard_index = std::hash<std::string>{}(pathname) % this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);

    // check if this object pool exist in metadata service.
    auto opm = find_object_pool(pathname);
    // remove it from local cache.
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);
    if(object_pool_metadata_cache.find(pathname) == object_pool_metadata_cache.end()) {
        // no entry in cache
        rlck.unlock();
    } else {
        // remove from cache
        rlck.unlock();
        std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
        object_pool_metadata_cache.erase(pathname);
        wlck.unlock();
    }
    if(opm.is_valid() && !opm.is_null() && !opm.deleted) {
        opm.deleted = true;
        opm.set_previous_version(CURRENT_VERSION, opm.version);  // only check previous_version_by_key
        return this->template put<CascadeMetadataService<CascadeTypes...>>(opm, METADATA_SERVICE_SUBGROUP_INDEX, metadata_service_shard_index);
    }

    // we didn't find the object pool, but we do the normal 'remove', which has no effect but return a version.
    dbg_default_warn("deleteing a non-existing objectpool:{}.", pathname);
    return this->template remove<CascadeMetadataService<CascadeTypes...>>(pathname, METADATA_SERVICE_SUBGROUP_INDEX, metadata_service_shard_index);
}

template <typename... CascadeTypes>
ObjectPoolMetadata<CascadeTypes...> ServiceClient<CascadeTypes...>::find_object_pool(const std::string& pathname) {
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);

    auto components = str_tokenizer(pathname);
    std::string prefix;
    for(const auto& comp : components) {
        prefix = prefix + PATH_SEPARATOR + comp;
        if(object_pool_metadata_cache.find(prefix) != object_pool_metadata_cache.end()) {
            return object_pool_metadata_cache.at(prefix);
        }
    }
    rlck.unlock();

    // refresh and try again.
    refresh_object_pool_metadata_cache();
    prefix = "";
    rlck.lock();
    for(const auto& comp : components) {
        prefix = prefix + PATH_SEPARATOR + comp;
        if(object_pool_metadata_cache.find(prefix) != object_pool_metadata_cache.end()) {
            return object_pool_metadata_cache.at(prefix);
        }
    }
    return ObjectPoolMetadata<CascadeTypes...>::IV;
}

template <typename... CascadeTypes>
std::vector<std::string> ServiceClient<CascadeTypes...>::list_object_pools(bool refresh) {
    if(refresh) {
        this->refresh_object_pool_metadata_cache();
    }

    std::vector<std::string> ret;
    std::shared_lock rlck(this->object_pool_metadata_cache_mutex);
    for(auto& op : this->object_pool_metadata_cache) {
        ret.emplace_back(op.first);
    }

    return ret;
}

template <typename... CascadeTypes>
template <typename SubgroupType>
bool ServiceClient<CascadeTypes...>::register_notification_handler(
        const cascade_notification_handler_t& handler,
        const uint32_t subgroup_index) {
    return register_notification_handler<SubgroupType>(handler, std::string{}, subgroup_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
bool ServiceClient<CascadeTypes...>::register_notification_handler(
        const cascade_notification_handler_t& handler,
        const std::string& object_pool_pathname,
        const uint32_t subgroup_index) {
    if(!is_external_client()) {
        throw derecho_exception(std::string(__PRETTY_FUNCTION__) + "Cannot register notification handler because external_group_ptr is null.");
    }

    std::unique_lock<std::mutex> type_registry_lock(this->notification_handler_registry_mutex);
    auto& per_type_registry = notification_handler_registry.template get<SubgroupType>();
    // Register Cascade's root handler:
    // if subgroup_index exists in the per_type_registry, Cascade's root handler is registered already.
    if(per_type_registry.find(subgroup_index) == per_type_registry.cend()) {
        per_type_registry.emplace(subgroup_index, SubgroupNotificationHandler<SubgroupType>{});
        // register to subgroup_caller
        auto& subgroup_caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        // to do ... register it.
        per_type_registry.at(subgroup_index).initialize(subgroup_caller);
    }
    auto& subgroup_handlers = per_type_registry.at(subgroup_index);

    // Register the handler
    std::lock_guard<std::mutex> subgroup_handlers_lock(*subgroup_handlers.object_pool_notification_handlers_mutex);
    bool ret = (subgroup_handlers.object_pool_notification_handlers.find(object_pool_pathname) != subgroup_handlers.object_pool_notification_handlers.cend());

    if(handler) {
        subgroup_handlers.object_pool_notification_handlers[object_pool_pathname] = handler;
    } else {
        subgroup_handlers.object_pool_notification_handlers[object_pool_pathname].reset();
    }
    return ret;
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
bool ServiceClient<CascadeTypes...>::type_recursive_register_notification_handler(
        uint32_t type_index,
        const cascade_notification_handler_t& handler,
        const std::string& object_pool_pathname,
        const uint32_t subgroup_index) {
    if(type_index == 0) {
        return this->template register_notification_handler<FirstType>(handler, object_pool_pathname, subgroup_index);
    } else {
        return this->template type_recursive_register_notification_handler<SecondType, RestTypes...>(
                type_index - 1, handler, object_pool_pathname, subgroup_index);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
bool ServiceClient<CascadeTypes...>::type_recursive_register_notification_handler(
        uint32_t type_index,
        const cascade_notification_handler_t& handler,
        const std::string& object_pool_pathname,
        const uint32_t subgroup_index) {
    if(type_index == 0) {
        return this->template register_notification_handler<LastType>(handler, object_pool_pathname, subgroup_index);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
bool ServiceClient<CascadeTypes...>::register_notification_handler(
        const cascade_notification_handler_t& handler,
        const std::string& object_pool_pathname) {
    auto opm = find_object_pool(object_pool_pathname);

    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }

    return this->template type_recursive_register_notification_handler<CascadeTypes...>(
            opm.subgroup_type_index, handler, object_pool_pathname, opm.subgroup_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::notify(
        const Blob& msg,
        const uint32_t subgroup_index,
        const node_id_t client_id) const {
    notify<SubgroupType>(msg, "", subgroup_index, client_id);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::notify(
        const Blob& msg,
        const std::string& object_pool_pathname,
        const uint32_t subgroup_index,
        const node_id_t client_id) const {
    if(is_external_client()) {
        throw derecho_exception(std::string(__PRETTY_FUNCTION__) + "Cannot notify an external client from an external client.");
    }

    auto& client_handle = group_ptr->template get_client_callback<SubgroupType>(subgroup_index);

    // TODO: redesign to avoid memory copies.
    CascadeNotificationMessage cascade_notification_message(object_pool_pathname, msg);
    derecho::NotificationMessage derecho_notification_message(CASCADE_NOTIFICATION_MESSAGE_TYPE, mutils::bytes_size(cascade_notification_message));
    mutils::to_bytes(cascade_notification_message, derecho_notification_message.body);

    client_handle.template p2p_send<RPC_NAME(notify)>(client_id, derecho_notification_message);
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
void ServiceClient<CascadeTypes...>::type_recursive_notify(
        uint32_t type_index,
        const Blob& msg,
        const std::string& object_pool_pathname,
        const uint32_t subgroup_index,
        const node_id_t client_id) const {
    if(type_index == 0) {
        this->template notify<FirstType>(msg, object_pool_pathname, subgroup_index, client_id);
    } else {
        this->template type_recursive_notify<SecondType, RestTypes...>(type_index - 1, msg, object_pool_pathname, subgroup_index, client_id);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
void ServiceClient<CascadeTypes...>::type_recursive_notify(
        uint32_t type_index,
        const Blob& msg,
        const std::string& object_pool_pathname,
        const uint32_t subgroup_index,
        const node_id_t client_id) const {
    if(type_index == 0) {
        this->template notify<LastType>(msg, object_pool_pathname, subgroup_index, client_id);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::notify(
        const Blob& msg,
        const std::string& object_pool_pathname,
        const node_id_t client_id) {
    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }
    this->template type_recursive_notify<CascadeTypes...>(opm.subgroup_type_index, msg, object_pool_pathname, opm.subgroup_index, client_id);
}

#ifdef ENABLE_EVALUATION

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::dump_timestamp(const std::string& filename, const uint32_t subgroup_index, const uint32_t shard_index) {
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
        } else {
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, filename);
            return subgroup_handle.template p2p_send<RPC_NAME(dump_timestamp_log)>(node_id, filename);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, filename);
        return caller.template p2p_send<RPC_NAME(dump_timestamp_log)>(node_id, filename);
    }
}

template <typename... CascadeTypes>
template <typename FirstType, typename SecondType, typename... RestTypes>
void ServiceClient<CascadeTypes...>::type_recursive_dump(
        uint32_t type_index,
        uint32_t subgroup_index,
        const std::string& filename) {
    if(type_index == 0) {
        this->template dump_timestamp<FirstType>(subgroup_index, filename);
    } else {
        this->template type_recursive_dump<SecondType, RestTypes...>(type_index - 1, subgroup_index, filename);
    }
}

template <typename... CascadeTypes>
template <typename LastType>
void ServiceClient<CascadeTypes...>::type_recursive_dump(
        uint32_t type_index,
        uint32_t subgroup_index,
        const std::string& filename) {
    if(type_index == 0) {
        this->template dump_timestamp<LastType>(subgroup_index, filename);
    } else {
        throw derecho::derecho_exception(std::string(__PRETTY_FUNCTION__) + ": type index is out of boundary.");
    }
}

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::dump_timestamp(const std::string& filename, const std::string& object_pool_pathname) {
    auto opm = find_object_pool(object_pool_pathname);
    if(!opm.is_valid() || opm.is_null() || opm.deleted) {
        throw derecho::derecho_exception("Failed to find object_pool:" + object_pool_pathname);
    }

    this->template type_recursive_dump<CascadeTypes...>(opm.subgroup_type_index, opm.subgroup_index, filename);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::dump_timestamp(const uint32_t subgroup_index, const std::string& filename) {
    uint32_t shards = get_number_of_shards<SubgroupType>(subgroup_index);
    for(uint32_t shard_index = 0; shard_index < shards; shard_index++) {
        auto result = this->template dump_timestamp<SubgroupType>(filename, subgroup_index, shard_index);
        result.get();
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::dump_timestamp_workaround(const std::string& filename, const uint32_t subgroup_index, const uint32_t shard_index, const node_id_t node_id) {
    if(!is_external_client()) {
        std::lock_guard<std::mutex> lck(this->group_ptr_mutex);
        if(static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(dump_timestamp_log_workaround)>(node_id, filename);
        } else {
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(dump_timestamp_log_workaround)>(node_id, filename);
        }
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        return caller.template p2p_send<RPC_NAME(dump_timestamp_log_workaround)>(node_id, filename);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<double> ServiceClient<CascadeTypes...>::perf_put(const uint32_t message_size, const uint64_t duration_sec, const uint32_t subgroup_index, const uint32_t shard_index) {
    if(!is_external_client()) {
        // 'perf_put' must be issued from an external client.
        throw derecho::derecho_exception{"perf_put must be issued from an external client."};
    } else {
        std::lock_guard<std::mutex> lck(this->external_group_ptr_mutex);
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index, 0);
        return caller.template p2p_send<RPC_NAME(perf_put)>(node_id, message_size, duration_sec);
    }
}
#endif  // ENABLE_EVALUATION

#ifndef __WITHOUT_SERVICE_SINGLETONS__
template <typename... CascadeTypes>
const std::vector<std::type_index> ServiceClient<CascadeTypes...>::subgroup_type_order{typeid(CascadeTypes)...};

template <typename... CascadeTypes>
const uint32_t ServiceClient<CascadeTypes...>::invalid_subgroup_type_index = 0xffffffff;

template <typename... CascadeTypes>
template <typename SubgroupType>
uint32_t ServiceClient<CascadeTypes...>::get_subgroup_type_index() {
    uint32_t index = 0;
    while(index < subgroup_type_order.size()) {
        if(std::type_index(typeid(SubgroupType)) == subgroup_type_order.at(index)) {
            return index;
        }
        index++;
    }
    return invalid_subgroup_type_index;
}

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::get_updated_group_cached_models_info(
                                    std::unordered_map<node_id_t, std::set<uint32_t>>& _group_cached_models_info, 
                                    std::unordered_map<node_id_t, uint64_t>& _group_available_memory,
                                    const std::unordered_map<uint32_t, MLModelStats>&   _local_ml_models_stats) {
    _group_cached_models_info.clear();
    std::vector<node_id_t> nodes = group_ptr->get_members();
    for(node_id_t node_id : nodes){
        uint64_t available_memory = GPU_MEMORY_SIZE;
        uint64_t encoded_models = group_ptr->get_cache_models_info(node_id);
        std::set<uint32_t> decoded_models;
        for(int k = 0; k < 64; ++k){
            if((encoded_models >> k) & 1){
                decoded_models.emplace(k);
                available_memory -= std::min(available_memory, _local_ml_models_stats.at((uint32_t)k).model_size); // avoid negative value
            }
        }
        _group_cached_models_info[node_id] = decoded_models;
        _group_available_memory[node_id] = available_memory;
    }
}

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::get_updated_group_queue_wait_times(std::unordered_map<node_id_t, uint64_t>& _group_queue_wait_times) {
    std::vector<node_id_t> nodes = group_ptr->get_members();
    for(node_id_t node_id : nodes) {
        uint64_t load_info = group_ptr->get_load_info(node_id);
        _group_queue_wait_times[node_id] = load_info;
    }
}

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::send_local_cached_models_info_to_group(std::set<uint32_t> _local_group_models) {
    uint64_t encoded_models = 0;
    for(int k = 0; k < 64; ++k) {
        if(_local_group_models.find(k) != _local_group_models.end()) {
            encoded_models |= 1 << k;
        }
    }
    /** lock is not needed here, since this only set the DerechoSST corresponding entry,
     * which will be put to all other nodes periodically through DerechoSST send_load_info_pred */
    group_ptr->set_my_cache_models_info(encoded_models);
}

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::send_local_queue_wait_time(uint64_t _local_queue_wait_time) {
    group_ptr->set_my_load_info(_local_queue_wait_time);
}

template <typename... CascadeTypes>
std::unique_ptr<ServiceClient<CascadeTypes...>> ServiceClient<CascadeTypes...>::service_client_singleton_ptr;

template <typename... CascadeTypes>
std::mutex ServiceClient<CascadeTypes...>::singleton_mutex;

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::initialize(derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* _group_ptr) {
    std::lock_guard<std::mutex> lock_guard(singleton_mutex);
    if(!service_client_singleton_ptr) {
        dbg_default_trace("initializing ServiceClient singleton as cascade member, group pointer={:p}", static_cast<void*>(_group_ptr));
        service_client_singleton_ptr = std::unique_ptr<ServiceClient<CascadeTypes...>>(new ServiceClient<CascadeTypes...>(_group_ptr));
    }
}

template <typename... CascadeTypes>
ServiceClient<CascadeTypes...>& ServiceClient<CascadeTypes...>::get_service_client() {
    if(!service_client_singleton_ptr) {
        std::lock_guard<std::mutex> lock_guard(singleton_mutex);
        // test again in case another thread has initialized it already.
        if(!service_client_singleton_ptr) {
            dbg_default_trace("initializing ServiceClient singleton as external client");
            service_client_singleton_ptr = std::unique_ptr<ServiceClient<CascadeTypes...>>(new ServiceClient<CascadeTypes...>(nullptr));
        }
    }
    return *service_client_singleton_ptr;
}
#endif  //__WITHOUT_SERVICE_SINGLETONS__

template <typename... CascadeTypes>
CascadeContext<CascadeTypes...>::CascadeContext() {
    stateless_action_queue_for_multicast.initialize();
    stateless_action_queue_for_p2p.initialize();
    prefix_registry_ptr = std::make_shared<PrefixRegistry<prefix_entry_t, PATH_SEPARATOR>>();
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::construct() {
    // 1 - create data path logic loader and register the prefixes. Ideally, this part should be done in the control
    // plane, where a centralized controller should issue the control messages to do load/unload.
    // TODO: implement the control plane.
    user_defined_logic_manager = UserDefinedLogicManager<CascadeTypes...>::create(this);
    auto dfgs = DataFlowGraph::get_data_flow_graphs();
    for(auto& dfg : dfgs) {
        for(auto& vertex : dfg.vertices) {
            for(auto& edge : vertex.second.edges) {
                register_prefixes(
                        {vertex.second.pathname},
                        vertex.second.shard_dispatchers.at(edge.first),
#ifdef HAS_STATEFUL_UDL_SUPPORT
                        vertex.second.stateful.at(edge.first),
#endif
                        vertex.second.hooks.at(edge.first),
                        edge.first,
                        user_defined_logic_manager->get_observer(
                                edge.first,  // UUID
                                vertex.second.configurations.at(edge.first)),
                        vertex.second.task_info.required_objects_pathnames,
                        edge.second,
                        vertex.second.task_info.expected_execution_timeus);
            }
            prefix_to_task_info.emplace(vertex.first, vertex.second.task_info);
            for(auto& model_info: vertex.second.task_info.models_info){
                MLModelStats model_stats = {model_info.model_size, 0, 0};
                this->local_ml_models_stats.emplace(model_info.model_id, model_stats);
            }
        }
    }
    // 2 - start the working threads
    is_running.store(true);
    local_available_memory.store(GPU_MEMORY_SIZE);
    local_queue_wait_time.store(0);
    uint32_t num_stateless_multicast_workers = 0;
    uint32_t num_stateless_p2p_workers = 0;
    // 2.1 - initialize stateless multicast workers.
    if(derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_STATELESS_WORKERS_MULTICAST) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_STATELESS_WORKERS_MULTICAST);
    } else {
        num_stateless_multicast_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_STATELESS_WORKERS_MULTICAST);
    }
    for(uint32_t i = 0; i < num_stateless_multicast_workers; i++) {
        // off_critical_data_path_thread_pool.emplace_back(std::thread(&CascadeContext<CascadeTypes...>::workhorse,this,i));
        stateless_workhorses_for_multicast.emplace_back(
                [this, i]() {
                    // set cpu affinity
                    if(this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.find(i) != this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.end()) {
                        cpu_set_t cpuset{};
                        CPU_ZERO(&cpuset);
                        for(auto core : this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.at(i)) {
                            CPU_SET(core, &cpuset);
                        }
                        if(pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
                            dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                        }
                    }
                    // call workhorse
                    this->workhorse(i, stateless_action_queue_for_multicast);
                });
    }
    // 2.2 -initialize stateless p2p workers.
    if(derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_STATELESS_WORKERS_P2P) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_STATELESS_WORKERS_P2P);
    } else {
        num_stateless_p2p_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_STATELESS_WORKERS_P2P);
    }
    for(uint32_t i = 0; i < num_stateless_p2p_workers; i++) {
        // off_critical_data_path_thread_pool.emplace_back(std::thread(&CascadeContext<CascadeTypes...>::workhorse,this,i));
        stateless_workhorses_for_p2p.emplace_back(
                [this, i]() {
                    // set cpu affinity
                    if(this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.find(i) != this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.end()) {
                        cpu_set_t cpuset{};
                        CPU_ZERO(&cpuset);
                        for(auto core : this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.at(i)) {
                            CPU_SET(core, &cpuset);
                        }
                        if(pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
                            dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                        }
                    }
                    // call workhorse
                    this->workhorse(i, stateless_action_queue_for_p2p);
                });
    }
#ifdef HAS_STATEFUL_UDL_SUPPORT
    uint32_t num_stateful_multicast_workers = 0;
    uint32_t num_stateful_p2p_workers = 0;
    // 2.3 - initialize stateful multicast workers
    if(derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_MULTICAST) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_MULTICAST);
    } else {
        num_stateful_multicast_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_MULTICAST);
    }
    stateful_action_queues_for_multicast.resize(num_stateful_multicast_workers);
    for(uint32_t i = 0; i < num_stateful_multicast_workers; i++) {
        // initialize local queue
        stateful_action_queues_for_multicast[i] = std::make_unique<struct action_queue>();
        stateful_action_queues_for_multicast.at(i)->initialize();
        stateful_workhorses_for_multicast.emplace_back(
                [this, i]() {
                    // set cpu affinity
                    if(this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.find(i) != this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.end()) {
                        cpu_set_t cpuset{};
                        CPU_ZERO(&cpuset);
                        for(auto core : this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.at(i)) {
                            CPU_SET(core, &cpuset);
                        }
                        if(pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
                            dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                        }
                    }
                    // call workhorse
                    this->workhorse(i, *stateful_action_queues_for_multicast.at(i));
                });
    }
    // 2.4 - initialize stateful p2p workers
    if(derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_P2P) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_P2P);
    } else {
        num_stateful_p2p_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_P2P);
    }
    stateful_action_queues_for_p2p.resize(num_stateful_p2p_workers);
    for(uint32_t i = 0; i < num_stateful_p2p_workers; i++) {
        // initialize local queue
        stateful_action_queues_for_p2p[i] = std::make_unique<struct action_queue>();
        stateful_action_queues_for_p2p.at(i)->initialize();
        stateful_workhorses_for_p2p.emplace_back(
                [this, i]() {
                    // set cpu affinity
                    if(this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.find(i) != this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.end()) {
                        cpu_set_t cpuset{};
                        CPU_ZERO(&cpuset);
                        for(auto core : this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.at(i)) {
                            CPU_SET(core, &cpuset);
                        }
                        if(pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
                            dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                        }
                    }
                    // call workhorse
                    this->workhorse(i, *stateful_action_queues_for_p2p.at(i));
                });
    }
    // 2.5 - initialize single threaded workers
    single_threaded_action_queue_for_multicast.initialize();
    single_threaded_action_queue_for_p2p.initialize();
    single_threaded_workhorse_for_multicast = std::thread(
            [this]() {
                // TODO:set cpu affinity
                // call workhorse
                // worker id 0xFFFFFFFF is reserved for single thread
                this->workhorse(0xFFFFFFFF, single_threaded_action_queue_for_multicast);
            });
    single_threaded_workhorse_for_p2p = std::thread(
            [this]() {
                // TODO:set cpu affinity
                // call workhorse
                // worker id 0xFFFFFFFF is reserved for single thread
                this->workhorse(0xFFFFFFFF, single_threaded_action_queue_for_p2p);
            });

#endif  // HAS_STATEFUL_UDL_SUPPORT
    // 3. initialize scheduler setting from config
    this->scheduler_type = derecho::getConfUInt32(CASCADE_CONTEXT_SCHEDULER_TYPE);
    this->reschedule_threashold_factor = derecho::getConfFloat(CASCADE_CONTEXT_RESCHEDULE_THREASHOLD_FACTOR);
    this->eviction_policy = derecho::getConfUInt32(CASCADE_CONTEXT_EVICTION_POLICY);
    this->num_models_fetch_for_future = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_MODELS_FETCH_FOR_FUTURE);
    this->num_actions_look_ahead_limit = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_ACTIONS_LOOK_AHEAD_LIMIT);
    // 3.1 - initialize scheduler worker 
    scheduler_workhorse = std::thread(
            [this]() {
                this->tide_scheduler_workhorse(0xFFFFFFFF, unscheduled_action_queue);
            });
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::workhorse(uint32_t worker_id, struct action_queue& aq) {
    pthread_setname_np(pthread_self(), ("cs_ctxt_t" + std::to_string(worker_id)).c_str());
    dbg_default_trace("Cascade context workhorse[{}] started", worker_id);
    while(is_running) {
        // waiting for an action
        Action action = std::move(aq.action_buffer_dequeue(is_running));
        action.fire(this,worker_id);
        if(!is_running) {
            do {
                action = std::move(aq.action_buffer_dequeue(is_running));
                if(!action) break;  // end of queue
                action.fire(this, worker_id);
            } while(true);
        }
        this->local_queue_wait_time -= std::min(action.expected_execution_timeus, this->local_queue_wait_time.load()); // avoid negative value
        this->get_service_client_ref().send_local_queue_wait_time(this->local_queue_wait_time);
        if(this->local_cached_models_info_updated.load()){
            this->send_local_cached_models_info();
        }
    }
    dbg_default_trace("Cascade context workhorse[{}] finished normally.", static_cast<uint64_t>(gettid()));
}


template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::tide_scheduler_workhorse(uint32_t worker_id, struct action_queue& aq) {
    pthread_setname_np(pthread_self(), ("cs_ctxt_t" + std::to_string(worker_id)).c_str());
    dbg_default_trace("Cascade context tide_scheduler_workhorse[{}] started", worker_id);
    while(is_running) {
        // waiting for an action
        Action action = std::move(aq.action_buffer_dequeue(is_running));
        if(!action) continue;
        if(!RESCHEDULE_JOINT_TASK){
            this->fire_scheduler(std::move(action), worker_id);
        }else{
            // case 1. entry task to be schedule the whole job
            if(action.required_object_pathnames.size() == 0){
                this->fire_scheduler(std::move(action), worker_id);
            }else{
                // 2. joint task to be rescheduled
                std::string vertex_pathname = (action.key_string).substr(0, action.prefix_length);
                bool scheduled;
                node_id_t scheduled_node = next_task_scheduled_node_id(scheduled, vertex_pathname, action.adfg, true);
                action.set_value_ptrs_num_reallocate(1);   // ALICIA TODO: double check this
                std::string trace_str = "_node:" + std::to_string(scheduled_node) + ", ";
                if(scheduled_node == this->get_service_client_ref().get_my_id()){
                    trace_str = " sending rescheduled_obj: " + action.key_string + " to the same node.";
                    this->post(std::move(action),action.stateful,action.is_trigger);
                }else{
                    // send all the intermediate result to the scheduled worker node
                    std::string trace_str = " sending rescheduled_objs: ";
                    for(size_t i = 0; i < action.required_object_pathnames.size(); i++){
                        auto* object_ptr = reinterpret_cast<ObjectWithStringKey*>(action.value_ptrs.at(i).get());
                        trace_str += "(key:" + object_ptr->get_key_ref() + ", prev_key:" + object_ptr->get_source_key() + "), ";
                        // object_ptr->set_num_reallocate(1);
                        this->get_service_client_ref().single_node_trigger_put((*object_ptr), scheduled_node);
                    }
                    trace_str += " to node " + std::to_string(scheduled_node);
                }
                dbg_default_trace(trace_str);
            }
        }
        if(!is_running) {
            do {
                action = std::move(aq.action_buffer_dequeue(is_running));
                if(!action) break;  
                this->fire_scheduler(std::move(action), worker_id);
            } while(true);
        }
    }
    dbg_default_trace("Cascade context workhorse[{}] finished normally.", static_cast<uint64_t>(gettid()));
}

/** Fire scheduler operation */
template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::fire_scheduler(Action&& action,uint32_t worker_id) {
    dbg_default_trace("CascadeContext<CascadeTypes...>::fire_scheduler() worker[{}] for action key: [{}]", worker_id, action.key_string);
    std::string vertex_pathname = (action.key_string).substr(0, action.prefix_length);
    uint64_t before_scheduler_us = get_time_us(true);
    switch(this->scheduler_type){
        case 0: {
            action.adfg = this->tide_scheduler(vertex_pathname);   // Note: remember to save adfg to objectWithStringKey at emit() 
        }break;
        case 1: {
            std::string key = (action.key_string).substr(action.prefix_length);
            action.adfg = this->hash_scheduler(vertex_pathname, key);
        }break;
        case 2: {
            action.adfg = this->heft_scheduler(vertex_pathname);
        }break;
        case 3:
            std::string key = (action.key_string).substr(action.prefix_length);
            action.adfg = this->hash_scheduler(vertex_pathname, key);
    }
    uint64_t after_scheduler_us = get_time_us(true);
    dbg_default_trace("~ vertex_pathname: {}, scheduled adfg: {}, time[{}]us", vertex_pathname, action.adfg, after_scheduler_us - before_scheduler_us);
    if(!action.adfg.empty()){
        ObjectWithStringKey* obj_ptr = reinterpret_cast<ObjectWithStringKey*>(action.value_ptrs.at(0).get());
        obj_ptr->adfg = action.adfg;
        size_t pos = action.adfg.find(",");
        if(pos == std::string::npos){
            pos = action.adfg.length();
        }
        node_id_t first_task_assigned_node_id = std::stoi(action.adfg.substr(0, pos));
        if(first_task_assigned_node_id == this->get_service_client_ref().get_my_id()){
            DataFlowGraph::Statefulness statefulness = action.stateful;
            bool is_trigger = action.is_trigger;
            this->post(std::move(action),statefulness,is_trigger);
        }else{
            this->get_service_client_ref().single_node_trigger_put((*obj_ptr), first_task_assigned_node_id);
        }
    }else{
        dbg_default_error("adfg is empty");
        return;
    }
    dbg_default_trace("Scheduler action finished normally.");
}


template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::action_queue::initialize() {
    action_buffer_head.store(0);
    action_buffer_tail.store(0);
}
#define ACTION_BUFFER_IS_FULL ((action_buffer_head) == ((action_buffer_tail + 1) % ACTION_BUFFER_SIZE))
#define ACTION_BUFFER_IS_EMPTY ((action_buffer_head) == (action_buffer_tail))
#define ACTION_BUFFER_DEQUEUE ((action_buffer_head) = (action_buffer_head + 1) % ACTION_BUFFER_SIZE)
#define ACTION_BUFFER_ENQUEUE ((action_buffer_tail) = (action_buffer_tail + 1) % ACTION_BUFFER_SIZE)
#define ACTION_BUFFER_HEAD (action_buffer[action_buffer_head])
#define ACTION_BUFFER_NEXT_TAIL (action_buffer[(action_buffer_tail) % ACTION_BUFFER_SIZE])

/* There is only one thread that enqueues. */
template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::action_queue::action_buffer_enqueue(Action&& action) {
    std::unique_lock<std::mutex> lck(action_buffer_slot_mutex);
    while(ACTION_BUFFER_IS_FULL) {
        dbg_default_warn("In {}: Critical data path waits for 10 ms. The action buffer is full! You are sending too fast or the UDL workers are too slow. This can cause a soft deadlock.", __PRETTY_FUNCTION__);
        action_buffer_slot_cv.wait_for(lck, 10ms, [this] { return !ACTION_BUFFER_IS_EMPTY; });
    }

    ACTION_BUFFER_NEXT_TAIL = std::move(action);
    ACTION_BUFFER_ENQUEUE;
    action_buffer_data_cv.notify_one();
}

template <typename... CascadeTypes>
bool CascadeContext<CascadeTypes...>::action_queue::action_buffer_emplace(Action& action) {
    std::unique_lock<std::mutex> lck(action_buffer_slot_mutex);
    size_t cursor = action_buffer_head.load();
    while(cursor != action_buffer_tail.load()){
        /** TODO: check for timestamp&version as well*/
        if(action_buffer[cursor].key_string == action.key_string){
            action_buffer[cursor].add_value_ptr(action.value_ptrs[0]);
            action_buffer_data_cv.notify_one();
            return true;
        }
        cursor = (cursor + 1) % ACTION_BUFFER_SIZE;
    }
    return false;
}   


/* All worker threads dequeues. */
template <typename... CascadeTypes>
Action CascadeContext<CascadeTypes...>::action_queue::action_buffer_dequeue(std::atomic<bool>& is_running) {
    std::unique_lock<std::mutex> lck(action_buffer_data_mutex);
    while(ACTION_BUFFER_IS_EMPTY && is_running) {
        action_buffer_data_cv.wait_for(lck, 10ms, [this, &is_running] { return (!ACTION_BUFFER_IS_EMPTY) || (!is_running); });
    }

    Action ret;
    // bool found_action = false;
    /**
     * TODO: current atomic action_buffer_head/tail implementation only works for single-threaded workhorse.
    */
    if(!ACTION_BUFFER_IS_EMPTY ) {
        if(ACTION_BUFFER_HEAD.received_all_preq_values()){
            ret = std::move(ACTION_BUFFER_HEAD);
            ACTION_BUFFER_DEQUEUE;
            action_buffer_slot_cv.notify_one();
        }else{
            size_t cursor = action_buffer_head.load();
            size_t cursor_end = action_buffer_tail.load();
            while(cursor != cursor_end){
                if(action_buffer[cursor].received_all_preq_values()){
                    ret = std::move(action_buffer[cursor]);
                    while(cursor != action_buffer_head.load()){
                        action_buffer[cursor] = std::move(action_buffer[(cursor + ACTION_BUFFER_SIZE - 1) % ACTION_BUFFER_SIZE]);
                        cursor = (cursor + ACTION_BUFFER_SIZE - 1) % ACTION_BUFFER_SIZE;
                    }
                    ACTION_BUFFER_DEQUEUE;
                    action_buffer_slot_cv.notify_one();
                    break;
                }
                cursor = (cursor + 1) % ACTION_BUFFER_SIZE;
            }
        }
    }

    return ret;
}

/* shutdown the action buffer */
template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::action_queue::notify_all() {
    action_buffer_data_cv.notify_all();
    action_buffer_slot_cv.notify_all();
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::destroy() {
    dbg_default_trace("Destroying Cascade context@{:p}.", static_cast<void*>(this));
    is_running.store(false);
    stateless_action_queue_for_multicast.notify_all();
    stateless_action_queue_for_p2p.notify_all();
    for(auto& th : stateless_workhorses_for_multicast) {
        if(th.joinable()) {
            th.join();
        }
    }
    for(auto& th : stateless_workhorses_for_p2p) {
        if(th.joinable()) {
            th.join();
        }
    }
    stateless_workhorses_for_multicast.clear();
    stateless_workhorses_for_p2p.clear();
#ifdef HAS_STATEFUL_UDL_SUPPORT
    for(auto& queue : stateful_action_queues_for_multicast) {
        queue->notify_all();
    }
    for(auto& queue : stateful_action_queues_for_p2p) {
        queue->notify_all();
    }
    for(auto& th : stateful_workhorses_for_multicast) {
        if(th.joinable()) {
            th.join();
        }
    }
    for(auto& th : stateful_workhorses_for_p2p) {
        if(th.joinable()) {
            th.join();
        }
    }
    stateful_workhorses_for_multicast.clear();
    stateful_workhorses_for_p2p.clear();
    if(single_threaded_workhorse_for_multicast.joinable()) {
        single_threaded_workhorse_for_multicast.join();
    }
    if(single_threaded_workhorse_for_p2p.joinable()) {
        single_threaded_workhorse_for_p2p.join();
    }
#endif  // HAS_STATEFUL_UDL_SUPPORT
    if(scheduler_workhorse.joinable()) {
        scheduler_workhorse.join();
    }
    dbg_default_trace("Cascade context@{:p} is destroyed.", static_cast<void*>(this));
}

template <typename... CascadeTypes>
ServiceClient<CascadeTypes...>& CascadeContext<CascadeTypes...>::get_service_client_ref() const {
    return ServiceClient<CascadeTypes...>::get_service_client();
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::register_prefixes(
        const std::unordered_set<std::string>& prefixes,
        const DataFlowGraph::VertexShardDispatcher shard_dispatcher,
#ifdef HAS_STATEFUL_UDL_SUPPORT
        const DataFlowGraph::Statefulness stateful,
#endif
        const DataFlowGraph::VertexHook hook,
        const std::string& user_defined_logic_id,
        const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr,
        const std::vector<std::string>& required_object_pathnames,
        const std::unordered_map<std::string, bool>& outputs,
        const uint64_t expected_execution_timeus) {
    for(const auto& prefix : prefixes) {
        prefix_registry_ptr->atomically_modify(
                prefix,
#ifdef HAS_STATEFUL_UDL_SUPPORT
                [&prefix, &shard_dispatcher, &stateful, &hook, &user_defined_logic_id, &ocdpo_ptr, &required_object_pathnames, &outputs, &expected_execution_timeus](const std::shared_ptr<prefix_entry_t>& entry) {
#else
                [&prefix, &shard_dispatcher, &hook, &user_defined_logic_id, &ocdpo_ptr, &required_object_pathnames, &outputs, &expected_execution_timeus](const std::shared_ptr<prefix_entry_t>& entry) {
#endif
                    std::shared_ptr<prefix_entry_t> new_entry;
                    if(entry) {
                        new_entry = std::make_shared<prefix_entry_t>(*entry);
                    } else {
                        new_entry = std::make_shared<prefix_entry_t>(prefix_entry_t{});
                    }
                    if(new_entry->find(user_defined_logic_id) == new_entry->end()) {
#ifdef HAS_STATEFUL_UDL_SUPPORT
                        new_entry->emplace(user_defined_logic_id, std::tuple{shard_dispatcher, stateful, hook, ocdpo_ptr, required_object_pathnames, outputs, expected_execution_timeus});
#else
                        new_entry->emplace(user_defined_logic_id, std::tuple{shard_dispatcher, hook, ocdpo_ptr, required_object_pathnames, outputs, expected_execution_timeus});
#endif
                    } else {
#ifdef HAS_STATEFUL_UDL_SUPPORT
                        std::get<5>(new_entry->at(user_defined_logic_id)).insert(outputs.cbegin(), outputs.cend());
#else
                        std::get<4>(new_entry->at(user_defined_logic_id)).insert(outputs.cbegin(), outputs.cend());
#endif
                    }
                    return new_entry;
                },
                true);
    }
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::unregister_prefixes(const std::unordered_set<std::string>& prefixes,
                                                          const std::string& user_defined_logic_id) {
    for(const auto& prefix : prefixes) {
        prefix_registry_ptr->atomically_modify(prefix,
                                               [&prefix, &user_defined_logic_id](const std::shared_ptr<prefix_entry_t>& entry) {
                                                   if(entry) {
                                                       std::shared_ptr<prefix_entry_t> new_value = std::make_shared<prefix_entry_t>(*entry);
                                                       new_value->erase(user_defined_logic_id);
                                                       return new_value;
                                                   } else {
                                                       return entry;
                                                   }
                                               });
    }
}

/* Note: On the same hardware, copying a shared_ptr spends ~7.4ns, and copying a raw pointer spends ~1.8 ns*/
template <typename... CascadeTypes>
match_results_t CascadeContext<CascadeTypes...>::get_prefix_handlers(const std::string& path) {
    match_results_t handlers;
    prefix_registry_ptr->collect_values_for_prefixes(
            path,
            [&handlers](const std::string& prefix, const std::shared_ptr<prefix_entry_t>& entry) {
                // handlers[prefix].insert(entry->cbegin(),entry->cend());
                if(entry) {
                    handlers.emplace(prefix, *entry);
                }
            });

    return handlers;
}

template <typename... CascadeTypes>
int64_t CascadeContext<CascadeTypes...>::get_task_ranking(const std::string& vertex_pathname) {
    auto it = this->prefix_to_task_info.find(vertex_pathname);
    if(it != this->prefix_to_task_info.end()) {
        auto& tasks_rankings = it->second.tasks_rankings_in_dfg;
        auto pos = std::find(tasks_rankings.begin(), tasks_rankings.end(), vertex_pathname) - tasks_rankings.begin();
        if((size_t)pos < tasks_rankings.size()){
            return (int64_t)pos;
        }
    }
    dbg_default_warn("task_ranking not exist for pathname[{}]", vertex_pathname);
    return -1;
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::find_handlers_and_local_post(ObjectWithStringKey& value){
/** This function is called by single_node_trigger_put if the next node is my_node_id.  Only handle trigger_put 
 * TODO: in current scheduler implementation, 
 * assume the handlers for the same pathname got assign to the same worker to be executed. 
 */                              
            const std::string& key = value.get_key_ref();
            size_t pos = key.rfind(PATH_SEPARATOR);
            std::string prefix;
            if(pos != std::string::npos) {
                // important: we need to keep the trailing PATH_SEPARATOR
                prefix = key.substr(0, pos + 1);
            }
            auto handlers = this->get_prefix_handlers(prefix);
            if(handlers.empty()) {
                return;
            }
            bool is_trigger = true; /** TODO: temp fix, check if it is trigger_put */
            // filter for normal put (put/put_and_forget)
            bool new_actions = false;
            {
                for(auto& per_prefix : handlers) {
                    // per_prefix.first is the matching prefix
                    // per_prefix.second is a set of handlers
                    for(auto it = per_prefix.second.cbegin(); it != per_prefix.second.cend();) {
                        // it->first is handler uuid
                        // it->second is a 5-tuple of shard dispatcher,stateful,hook,ocdpo,and outputs;
#ifdef HAS_STATEFUL_UDL_SUPPORT
                        if((std::get<2>(it->second) == DataFlowGraph::VertexHook::ORDERED_PUT && is_trigger) || (std::get<2>(it->second) == DataFlowGraph::VertexHook::TRIGGER_PUT && !is_trigger)) {
#else
                        if((std::get<1>(it->second) == DataFlowGraph::VertexHook::ORDERED_PUT && is_trigger) || (std::get<1>(it->second) == DataFlowGraph::VertexHook::TRIGGER_PUT && !is_trigger)) {
#endif
                            // not my hook, skip it.
                            per_prefix.second.erase(it++);
#ifdef HAS_STATEFUL_UDL_SUPPORT
                        } else if((std::get<2>(it->second) != DataFlowGraph::VertexHook::ORDERED_PUT) && is_trigger) {
#else
                        } else if((std::get<1>(it->second) != DataFlowGraph::VertexHook::ORDERED_PUT) && is_trigger) {
#endif
                            new_actions = true;
                            it++;
                        } else {
                            switch(std::get<0>(it->second)) {
                                case DataFlowGraph::VertexShardDispatcher::ONE:
                                    new_actions = true;
                                    it++;
                                    break;
                                case DataFlowGraph::VertexShardDispatcher::ALL:
                                    new_actions = true;
                                    it++;
                                    break;
                                default:
                                    per_prefix.second.erase(it++);
                                    break;
                            }
                        }
                    }
                }
            }
            if(!new_actions) {
                return;
            }
            // copy data
            auto value_ptr = std::make_shared<ObjectWithStringKey>(value);
            node_id_t sender_id = this->get_service_client_ref().get_my_id();
            // create actions
            for(auto& per_prefix : handlers) {
                // per_prefix.first is the matching prefix
                // per_prefix.second is a set of handlers
                for(const auto& handler : per_prefix.second) {
                    // handler.first is handler uuid
                    // handler.second is a 4,5-tuple of shard dispatcher,stateful,hook,ocdpo,and outputs;
                    Action action(
                            sender_id,
                            key,
                            per_prefix.first.size(),
                            value.get_version(),
                            value.get_adfg(),
#ifdef HAS_STATEFUL_UDL_SUPPORT
                            std::get<3>(handler.second),  // ocdpo
#else
                            std::get<2>(handler.second),  // ocdpo
#endif
                            value_ptr,
#ifdef HAS_STATEFUL_UDL_SUPPORT
                            std::get<4>(handler.second),  // required object pathnames
                            std::get<5>(handler.second),  // outputs
                            std::get<6>(handler.second),  // expected_execution_timeus
#else
                            std::get<3>(handler.second),  // required object pathnames
                            std::get<4>(handler.second),  // outputs
                            std::get<5>(handler.second),  // expected_execution_timeus
#endif
                    std::get<1>(handler.second),  // stateful
                    is_trigger);
                    bool joint_task_to_reallocate = false;
                    if (RESCHEDULE_JOINT_TASK){
                        if(action.required_object_pathnames.size() > 1 && value_ptr->get_num_reallocate() == 0){
                            dbg_default_trace("find_handlers_and_local_post() escheduled joint task({}) ", action.key_string);
                            this->post_to_scheduler(std::move(action));
                            joint_task_to_reallocate = true;
                        }
                    }
                    if (!joint_task_to_reallocate){
                        // post action
#ifdef HAS_STATEFUL_UDL_SUPPORT
                        this->post(std::move(action), std::get<1>(handler.second), is_trigger);
#else
                        this->post(std::move(action), is_trigger);
#endif//HAS_STATEFUL_UDL_SUPPORT
                    }
                }
            }

}


template <typename... CascadeTypes>
#ifdef HAS_STATEFUL_UDL_SUPPORT
bool CascadeContext<CascadeTypes...>::post(Action&& action, DataFlowGraph::Statefulness stateful, bool is_trigger) {
#else
bool CascadeContext<CascadeTypes...>::post(Action&& action, bool is_trigger) {
#endif  // HAS_STATEFUL_UDL_SUPPORT
    dbg_default_trace("Posting an action to Cascade context@{:p}.", static_cast<void*>(this));
    if(is_running) {
        if(is_trigger) {
#ifdef HAS_STATEFUL_UDL_SUPPORT
            switch(stateful) {
                case DataFlowGraph::Statefulness::STATEFUL: {
                    uint32_t thread_index = std::hash<std::string>{}(action.key_string) % stateful_action_queues_for_p2p.size();
                    stateful_action_queues_for_p2p[thread_index]->action_buffer_enqueue(std::move(action));
                } break;
                case DataFlowGraph::Statefulness::STATELESS:
#endif
                    stateless_action_queue_for_p2p.action_buffer_enqueue(std::move(action));
#ifdef HAS_STATEFUL_UDL_SUPPORT
                    break;
                case DataFlowGraph::Statefulness::SINGLETHREADED:
                    bool emplace_to_existing_queued_task = false;
                    // check if action is in single_threaded_action_queue_for_p2p, if it is add_value_ptr to that action                    
                    if(action.required_object_pathnames.size() > 1){
                        emplace_to_existing_queued_task = single_threaded_action_queue_for_p2p.action_buffer_emplace(action);
                        if(!emplace_to_existing_queued_task){
                            single_threaded_action_queue_for_p2p.action_buffer_enqueue(std::move(action));
                        }
                    }else{
                        single_threaded_action_queue_for_p2p.action_buffer_enqueue(std::move(action));
                    }
                    if(!emplace_to_existing_queued_task){
                        this->local_queue_wait_time += action.expected_execution_timeus;
                        this->get_service_client_ref().send_local_queue_wait_time(this->local_queue_wait_time);
                    }
                    break;
            }
#endif
        } else {
#ifdef HAS_STATEFUL_UDL_SUPPORT
            switch(stateful) {
                case DataFlowGraph::Statefulness::STATEFUL: {
                    uint32_t thread_index = std::hash<std::string>{}(action.key_string) % stateful_action_queues_for_multicast.size();
                    stateful_action_queues_for_multicast[thread_index]->action_buffer_enqueue(std::move(action));
                } break;
                case DataFlowGraph::Statefulness::STATELESS:
#endif
                    stateless_action_queue_for_multicast.action_buffer_enqueue(std::move(action));
#ifdef HAS_STATEFUL_UDL_SUPPORT
                    break;
                case DataFlowGraph::Statefulness::SINGLETHREADED:
                    bool emplace_to_existing_queued_task = false;
                    // check if action is in single_threaded_action_queue_for_p2p, if it is add_value_ptr to that action                    
                    if(action.required_object_pathnames.size() > 1){
                        emplace_to_existing_queued_task = single_threaded_action_queue_for_multicast.action_buffer_emplace(action);
                        if(!emplace_to_existing_queued_task){
                            single_threaded_action_queue_for_multicast.action_buffer_enqueue(std::move(action));
                        }
                    }else{
                        single_threaded_action_queue_for_multicast.action_buffer_enqueue(std::move(action));
                    }
                    if(!emplace_to_existing_queued_task){
                        this->local_queue_wait_time += action.expected_execution_timeus;
                        this->get_service_client_ref().send_local_queue_wait_time(this->local_queue_wait_time);
                    }
                    break;
            }
#endif
        }
    } else {
        dbg_default_warn("Failed to post to Cascade context@{:p} because it is not running.", static_cast<void*>(this));
        return false;
    }   
    dbg_default_trace("Action posted to Cascade context@{:p}.", static_cast<void*>(this));
    return true;
}

template <typename... CascadeTypes>
bool CascadeContext<CascadeTypes...>::post_to_scheduler(Action&& action) {
    bool emplace_to_existing_queued_task = false;
    if(RESCHEDULE_JOINT_TASK && action.required_object_pathnames.size() > 1){
        emplace_to_existing_queued_task = unscheduled_action_queue.action_buffer_emplace(action);
    }
    if(!emplace_to_existing_queued_task){
        unscheduled_action_queue.action_buffer_enqueue(std::move(action));
    }
    return true;
}

template <typename... CascadeTypes>
size_t CascadeContext<CascadeTypes...>::stateless_action_queue_length_p2p() {
    return (stateless_action_queue_for_p2p.action_buffer_tail - stateless_action_queue_for_multicast.action_buffer_head + ACTION_BUFFER_SIZE) % ACTION_BUFFER_SIZE;
}

template <typename... CascadeTypes>
size_t CascadeContext<CascadeTypes...>::stateless_action_queue_length_multicast() {
    return (stateless_action_queue_for_multicast.action_buffer_tail - stateless_action_queue_for_multicast.action_buffer_head + ACTION_BUFFER_SIZE) % ACTION_BUFFER_SIZE;
}

template <typename... CascadeTypes>
uint64_t CascadeContext<CascadeTypes...>::check_queue_wait_time(node_id_t node_id) {
    if (node_id == this->get_service_client_ref().get_my_id()){
        return local_queue_wait_time.load();
    }
    uint64_t cur_us = get_time_us(true);
    auto interval = 60.0 * 1000000 / derecho::getConfUInt32(LOAD_INFO_DISSEMINATION_RATE);
    if(cur_us - last_group_queue_wait_times_update_timeus > interval) {
        last_group_queue_wait_times_update_timeus = cur_us;
        std::unique_lock<std::shared_mutex> wlck(this->group_queue_wait_times_mutex);
        this->get_service_client_ref().get_updated_group_queue_wait_times(this->group_queue_wait_times);
    }
    std::shared_lock rlck(this->group_queue_wait_times_mutex);
    if (this->group_queue_wait_times.find(node_id) == this->group_queue_wait_times.end()){
        dbg_default_warn("CascadeContext::check_queue_wait_time unable to find node {}", node_id);
    }
    auto wait_time = this->group_queue_wait_times[node_id];
    /** for test purposes*/
    // dbg_default_trace(this->local_cached_info_dump());
    return wait_time;
}

template <typename... CascadeTypes>
bool CascadeContext<CascadeTypes...>::check_if_model_in_gpu(node_id_t node_id, uint32_t model_id) {
    if(node_id ==  this->get_service_client_ref().get_my_id()){
        std::shared_lock rlck(this->local_cached_model_info_mutex);
        auto it = std::find(this->local_cached_model_ids.begin(), this->local_cached_model_ids.end(), model_id);
        return it != this->local_cached_model_ids.end();
    }
    uint64_t cur_us = get_time_us(true);
    auto interval = 60.0 * 1000000 / derecho::getConfUInt32(CACHE_INFO_DISSEMINATION_RATE);
    if(cur_us - last_group_cached_models_info_update_timeus > interval) {
        last_group_cached_models_info_update_timeus = cur_us;
        std::unique_lock<std::shared_mutex> wlck(this->group_cached_models_info_mutex);
        this->get_service_client_ref().get_updated_group_cached_models_info(this->group_cached_models_info, 
                                                                            this->group_available_memory,
                                                                            this->local_ml_models_stats);
    }
    std::shared_lock rlck(this->group_cached_models_info_mutex);
    bool exist_model_in_gpu = this->group_cached_models_info[node_id].find(model_id) != this->group_cached_models_info[node_id].end();
    return exist_model_in_gpu;
}

template <typename... CascadeTypes>
std::vector<DataFlowGraph::MLModelInfo> CascadeContext<CascadeTypes...>::get_required_models_info(std::string pathname){
    if(pathname.back() != PATH_SEPARATOR){
        pathname += PATH_SEPARATOR;
    }
    auto it = this->prefix_to_task_info.find(pathname);
    if(it != this->prefix_to_task_info.end()) {
        return it->second.models_info;   
    }
    return std::vector<DataFlowGraph::MLModelInfo>();
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::send_local_cached_models_info(){
    std::shared_lock rlck(this->local_cached_model_info_mutex);
    std::set<uint32_t> _model_ids(this->local_cached_model_ids.begin(), this->local_cached_model_ids.end());
    bool ALICIA_DEBUG = true;
    if(ALICIA_DEBUG){
        assert(_model_ids.size() == this->local_cached_model_ids.size() && "local_cached_model_ids contains duplication");
        assert(this->local_available_memory.load() < GPU_MEMORY_SIZE && "gpu_available_memory is larger than GPU_MEMORY_SIZE");
    }
    this->get_service_client_ref().send_local_cached_models_info_to_group(_model_ids);
    this->local_cached_models_info_updated.store(false);
}

template <typename... CascadeTypes>
uint64_t CascadeContext<CascadeTypes...>::look_ahead_queue_required_models(std::vector<uint32_t>& models_to_fetch_for_future,
                                                                    const std::vector<DataFlowGraph::MLModelInfo>& required_model_info,
                                                                    const std::uint64_t& required_memory,
                                                                    const std::uint64_t& current_gpu_available_memory){
    size_t cur_num_models_to_fetch_for_future = 0;
    size_t num_actions_look_ahead = 0;
    uint64_t total_required_memory = 0;
    uint64_t available_memory_for_prefetch = current_gpu_available_memory - required_memory;
    size_t cur_pos = single_threaded_action_queue_for_p2p.action_buffer_head.load();
    size_t end_pos = single_threaded_action_queue_for_p2p.action_buffer_tail.load();
    while(cur_pos != end_pos){
        auto& action = single_threaded_action_queue_for_p2p.action_buffer[cur_pos];
        std::string pathname = (action.key_string).substr(0, action.prefix_length);
        std::vector<DataFlowGraph::MLModelInfo> models_info = this->get_required_models_info(pathname);
        for(const auto& model: models_info){
            if (cur_num_models_to_fetch_for_future < this->num_models_fetch_for_future){
                // check if model is already in cache
                int32_t model_id = model.model_id;
                auto it_current_required = std::find_if(required_model_info.begin(), required_model_info.end(),[model_id](const DataFlowGraph::MLModelInfo& r_model){
                                                return r_model.model_id == model_id;    
                                            });
                auto it_local_cache = std::find(this->local_cached_model_ids.begin(), this->local_cached_model_ids.end(), model_id);
                if(it_current_required == required_model_info.end() && it_local_cache == this->local_cached_model_ids.end()){
                    /** Stop looking ahead for more models, once see a required model cannot no longer fit to available memory.
                     *  This is to avoid unnecesary model movement. Want to pre-fetch the models required by earlier task, before pre-fetching models for later tasks
                     *  Since fetching later required models before earlier ones, may lead eviction of them to make room for earlier ones
                    */
                    if (total_required_memory + model.model_size > available_memory_for_prefetch){
                        return total_required_memory;
                    }else{
                        models_to_fetch_for_future.emplace_back(model.model_id);
                        this->local_cached_model_ids.emplace_back(model.model_id);
                        cur_num_models_to_fetch_for_future ++;
                        total_required_memory += model.model_size;
                        // stop looking ahead if enough models are found
                        if (cur_num_models_to_fetch_for_future >= this->num_models_fetch_for_future){
                            return total_required_memory;
                        }
                    }
                }
            }
        }
        cur_pos = (cur_pos + 1) % ACTION_BUFFER_SIZE;
        // only look ahead for NUM_ACTIONS_LOOK_AHEAD actions, not to overlook ahead
        num_actions_look_ahead ++;
        if (num_actions_look_ahead >= this->num_actions_look_ahead_limit){
            break;
        }
    }
    return total_required_memory;
}

template <typename... CascadeTypes>
uint64_t CascadeContext<CascadeTypes...>::select_models_to_evict(std::vector<uint32_t>& models_to_evict,
                                                            const std::vector<DataFlowGraph::MLModelInfo>& required_model_info, 
                                                            const std::uint64_t& required_memory,
                                                            const std::uint64_t& current_gpu_available_memory){
    // 1. order local_cached_model_ids according to eviction policy
    if(this->eviction_policy == 0){
        // FIFO eviction policy
        dbg_default_trace("Select_models_to_evict() uses FIFO eviction policy");
    }else if(this->eviction_policy == 1){
        // LOOK_AHEAD eviction policy: prioritize to evict the models that are not going to be used in the near future.
        dbg_default_trace("Select_models_to_evict() uses LOOK_AHEAD eviction policy");
        // 1.1. collect index_map(model_id->position in task_queue)
        std::unordered_map<uint32_t, size_t> index_map;
        size_t cur_pos = single_threaded_action_queue_for_p2p.action_buffer_head.load();
        size_t end_pos = single_threaded_action_queue_for_p2p.action_buffer_tail.load();
        size_t index = 0;
        while(cur_pos != end_pos){
            auto& action = single_threaded_action_queue_for_p2p.action_buffer[cur_pos];
            std::string pathname = (action.key_string).substr(0, action.prefix_length);
            std::vector<DataFlowGraph::MLModelInfo> models_info = this->get_required_models_info(pathname);
            for(const auto& model: models_info){
                if(index_map.find(model.model_id) == index_map.end()){
                    index_map[model.model_id] = index;
                    index ++;
                }
            }
            cur_pos = (cur_pos + 1) % ACTION_BUFFER_SIZE;
        }
        /** 1.2. reorder local_cached_model_ids,
         * such that the earlier the model appear in the task_queue, 
         * the later it appears in local_cached_model_ids, less likely to be evicted
         */
        std::sort(this->local_cached_model_ids.begin(), this->local_cached_model_ids.end(), 
                    [&index_map](int model1, int model2){
            auto it1 = index_map.find(model1);
            auto it2 = index_map.find(model2);
            if(it1 == index_map.end()){
                return true;
            }
            if(it2 == index_map.end()){
                return false;
            }
            return it1->second > it2->second;
        });
    }
    // 2. select models to evict from cache until required_memory is satisfied
    // uint64_t GPU_avaialble_memory = this->local_available_memory.load();
    uint64_t freed_memory = 0;
    auto it = this->local_cached_model_ids.begin();
    while( it != this->local_cached_model_ids.end() ) {
        if(required_memory <= current_gpu_available_memory + freed_memory){
            return freed_memory;
        }
        int32_t model_id = (int32_t)*it;
        // check if the model is required by this task, if so then skip it
        auto required_model_it = std::find_if(required_model_info.begin(), required_model_info.end(),[model_id](const DataFlowGraph::MLModelInfo& model){
                                                return model.model_id == model_id;    
                                            });
        if(required_model_it != required_model_info.end()){
            it ++;
            continue;
        }
        models_to_evict.emplace_back(model_id);
        freed_memory += this->local_ml_models_stats[model_id].model_size;
        it = this->local_cached_model_ids.erase(it); 
    }
    return freed_memory;
}

template <typename... CascadeTypes>
bool CascadeContext<CascadeTypes...>::models_to_fetch_and_evict(std::string pathname,
                                                                std::vector<uint32_t>& models_to_fetch,
                                                                std::vector<uint32_t>& models_to_evict,
                                                                std::vector<uint32_t>& models_to_fetch_for_future){
    std::unique_lock wlck(this->local_cached_model_info_mutex);
    uint64_t required_memory = 0;
    std::vector<DataFlowGraph::MLModelInfo> required_model_info = this->get_required_models_info(pathname);
    if(required_model_info.empty()){
        return true;
    }
    // 1. Collect the model IDs required but not currently in GPU memory
    for(const auto& model: required_model_info){
        if(std::find(this->local_cached_model_ids.begin(), this->local_cached_model_ids.end(), model.model_id) == this->local_cached_model_ids.end()){
            models_to_fetch.emplace_back(model.model_id);
            required_memory += model.model_size;
        }
    }
    // 2. select models to be removed from GPU memory
    uint64_t available_memory = this->local_available_memory.load();
    if(required_memory > available_memory){
        uint64_t freed_memory = this->select_models_to_evict(models_to_evict, required_model_info, required_memory, available_memory);
        available_memory += freed_memory;
    }else if(this->eviction_policy == 1){
        uint64_t concurrent_prefetch_required_memory = this->look_ahead_queue_required_models(models_to_fetch_for_future, required_model_info, required_memory, available_memory);
        // abort prefetching if not enough memory
        if(concurrent_prefetch_required_memory + required_memory > available_memory){
            models_to_fetch_for_future.clear();
        }else{
            required_memory += concurrent_prefetch_required_memory;
        }
    }
    // double check if GPU memory is enough to hold the required models
    if(required_memory > available_memory){
        dbg_default_warn("In {}, GPU memory is not enough to hold the required models", __PRETTY_FUNCTION__);
        std::cout << "GPU memory is not enough to hold the required models" << std::endl;
        return false;
    }
    // 3. updated this node's local_cached_model_ids and local_available_memory
    uint64_t new_gpu_available_memory = available_memory - required_memory;
    if(!models_to_fetch.empty() || !models_to_evict.empty() || !models_to_fetch_for_future.empty()){
        this->local_cached_model_ids.insert(this->local_cached_model_ids.end(), models_to_fetch.begin(), models_to_fetch.end());
        this->local_available_memory.store(new_gpu_available_memory);
        this->local_cached_models_info_updated.store(true);
    }
    return true;
}


template <typename... CascadeTypes>
std::string CascadeContext<CascadeTypes...>::local_cached_info_dump() {
    std::string out ;
    out = out + "\nCascadeContext:\n"
        + "\tCached group info:\n"
        + "\tnode_id,  queue_wait_time, cached_models \n";
    std::shared_lock rlck_wait_time(this->group_queue_wait_times_mutex);
    std::shared_lock rlck_models_gpu(this->group_cached_models_info_mutex);
    for(auto& [node_id, wait_time] : this->group_queue_wait_times) {
        out = out + std::to_string(node_id) + ", " + std::to_string(wait_time) + ", ";
        out = out + "[";
        // std::set<uint32_t>::iterator itr;
        for(auto models : this->group_cached_models_info[node_id]) {
            out = out + std::to_string(models) + ", ";
        }
        out = out + "]\n";
    }
    return out;
}


template <typename... CascadeTypes>
node_id_t CascadeContext<CascadeTypes...>::next_task_scheduled_node_id(bool& scheduled, 
                                                                       const std::string& task_name, 
                                                                       const std::string& adfg,
                                                                       bool gathered_by_receiver){
    int64_t task_rank = this->get_task_ranking(task_name);
    scheduled = false;
    node_id_t scheduled_node_id = 0;
    // 1. Check if the task is scheduled, and get the allocated node_id in adfg using task_rank
    if(task_rank != -1){
        int64_t position = task_rank;
        std::regex rgx(",");
        std::sregex_token_iterator end;
        std::sregex_token_iterator iter(adfg.begin(), adfg.end(), rgx, -1);
        for(; iter != end; iter++){
            if(position == 0){
                scheduled = true;
                scheduled_node_id = static_cast<uint32_t>(std::stoul(*iter));
                break;
            }
            position --;
        }
    }
    if(!scheduled){
        dbg_default_warn("CascadeContext::next_task_scheduled_node_id task {} is not scheduled", task_name);
        return 0;
    }
    // Only TIDE and JIT schedule dynamically
    if(this->scheduler_type == 1 || this->scheduler_type == 2){ // experiment purposes
        return scheduled_node_id;
    }
    // 2. Check if the intially assigned worker is still a good choice. If not, re-schedule
    auto& task_info = prefix_to_task_info[task_name];
    uint64_t wait_threashold = 0;  // JIT(schedule_type==3) reschedules any task to the node with shortest queue wait time
    // TIDE scheduler prioritize the original plan, only reschedule when wait_threashold exceed the limit
    if (this->scheduler_type == 0){
        wait_threashold = this->reschedule_threashold_factor * task_info.expected_execution_timeus;
    }
    // Two cases require reschedule.
    // case 2.1 if it is not a joint task in the dfg, and the previously scheduled_node_id has waittime longer than threashold
    bool require_reschedule_non_joint = (this->check_queue_wait_time(scheduled_node_id) > wait_threashold) & (task_info.required_objects_pathnames.size() < 2); 
    // case 2.2 when scheduling policy request RESCHEDULE_JOINT_TASK, if it is a joint task (that is collected by receiver) 
    bool require_reschedule_joint = ((RESCHEDULE_JOINT_TASK == 1) & (task_info.required_objects_pathnames.size() > 1) & gathered_by_receiver);
    // 3. Re-schedule the task
    if(require_reschedule_non_joint || require_reschedule_joint){
        node_id_t local_node_id = this->get_service_client_ref().get_my_id();
        std::vector<node_id_t> workers_set = this->get_service_client_ref().get_members();
        workers_set.erase(std::remove(workers_set.begin(), workers_set.end(), 0), workers_set.end()); // TODO: change this to config/auto condition phrase
        uint64_t min_wait_time = UINT64_MAX;
        for(const auto& cur_worker: workers_set){
            uint64_t cur_wait_time = this->check_queue_wait_time(cur_worker);
            uint64_t model_fetch_time = 0;
            uint64_t cur_available_memory = this->group_available_memory[cur_worker];
            if(cur_worker == local_node_id){
                cur_available_memory = this->local_available_memory;
            }
            for(auto& model_info: task_info.models_info){
                if(!this->check_if_model_in_gpu(cur_worker, model_info.model_id)){
                    model_fetch_time += host_to_GPU_delay(model_info.model_size);
                    // count for delay because of model eviction due to memory limit
                    if (CONSIDER_GPU_LIMIT){
                        if(cur_available_memory < model_info.model_size){
                            model_fetch_time += host_to_GPU_delay(model_info.model_size);   // use host_to_GPU_delay to estimate model eviction delay
                        }
                    }
                }
            }
            if(cur_wait_time + model_fetch_time < min_wait_time){
                min_wait_time = cur_wait_time + model_fetch_time;
                scheduled_node_id = cur_worker;
            }
        }
    }
    dbg_default_trace("CascadeContext::next_task_scheduled_node_id() rescheduled {} to node {}", task_name, scheduled_node_id);
    return scheduled_node_id;
}


template <typename... CascadeTypes>
std::string CascadeContext<CascadeTypes...>::tide_scheduler(std::string entry_prefix){
    // vertex pathname -> (node_id, finish_time(us))
    // in the algorithm denote task ~ vertex pathname
    std::unordered_map<std::string, std::pair<node_id_t,uint64_t>> allocated_tasks_info;
    std::vector<node_id_t> workers_set = this->get_service_client_ref().get_members();
    /** remove node_id 0 from worker_set, since it is metadata server
     * This require node0 to have both metadata service and at least one of VCSS/PCSS/TCSS shard, so that it can process trigger_put. 
     * Otherwise, uncomment below line.
     * TODO: change this to config/auto condition phrase
    */
    workers_set.erase(std::remove(workers_set.begin(), workers_set.end(), 0), workers_set.end());
    node_id_t local_node_id = this->get_service_client_ref().get_my_id();
    std::unordered_map<node_id_t, uint64_t>  c_group_available_memory = this->group_available_memory;
    c_group_available_memory[local_node_id] = this->local_available_memory.load();

    auto it = this->prefix_to_task_info.find(entry_prefix);
    if(it == this->prefix_to_task_info.end()){
        dbg_default_error("CascadeContext::tide_scheduler task_info is empty");
        return "";
    }
    DataFlowGraph::TaskInfo& entry_task_info = this->prefix_to_task_info[entry_prefix];

    uint64_t cur_us = get_time_us(true);
    std::vector<std::string>& tasks_rankings = entry_task_info.tasks_rankings_in_dfg;
    std::unordered_map<node_id_t, uint64_t> earliest_available_times;
    for(auto& node_id : workers_set){
        earliest_available_times[node_id] = cur_us + this->check_queue_wait_time(node_id);
    }
    for(auto& task_name: tasks_rankings){
        auto& task_info = this->prefix_to_task_info[task_name];
        // 0. PRE-COMPUTE (used later by 2. case1) get the earliest start time, suppose all preq_tasks need to transfer data
        node_id_t selected_worker_id = INVALID_NODE_ID;
	    uint64_t earliest_start_time = UINT64_MAX;
        uint64_t fetching_model_size = 0;
        for(const auto& cur_worker: workers_set){
            uint64_t cur_earliest_start_time = cur_us;
            uint64_t inputs_arrival_time = cur_us;
            if(task_name == tasks_rankings[0] && cur_worker != local_node_id){
                    inputs_arrival_time += CPU_to_CPU_delay(task_info.input_size);
            }
            for(std::string& preq_task_name: task_info.required_objects_pathnames){
                auto& preq_task_info = this->prefix_to_task_info[preq_task_name];
                auto& alloc_info = allocated_tasks_info[preq_task_name];
                uint64_t arrival_time = alloc_info.second;
                if(cur_worker != alloc_info.first){
                    arrival_time += CPU_to_CPU_delay(preq_task_info.output_size);
                }
                inputs_arrival_time = std::max(inputs_arrival_time, arrival_time);
            }
            cur_earliest_start_time = std::max(cur_earliest_start_time, inputs_arrival_time);
            uint64_t model_fetch_time = 0;
            uint64_t cur_fetching_model_size = 0;
            for(auto& model_info: task_info.models_info){
                if(!this->check_if_model_in_gpu(cur_worker, model_info.model_id)){
                    model_fetch_time += host_to_GPU_delay(model_info.model_size);
                    // count for delay because of model eviction due to memory limit
                    if (CONSIDER_GPU_LIMIT){
                            if(c_group_available_memory[cur_worker] < model_info.model_size){
                            model_fetch_time += host_to_GPU_delay(model_info.model_size);   // use host_to_GPU_delay to estimate model eviction delay
                        }
                    }
                    cur_fetching_model_size += model_info.model_size;
                }
            }
            cur_earliest_start_time = std::max(earliest_available_times[cur_worker], cur_earliest_start_time) + model_fetch_time;
            if(cur_earliest_start_time < earliest_start_time){
                earliest_start_time = cur_earliest_start_time;
                selected_worker_id = cur_worker;
                fetching_model_size = cur_fetching_model_size;
            }
        }
        if(selected_worker_id == INVALID_NODE_ID){
            dbg_default_error("CascadeContext::tide_scheduler selected_worker_id == -1");
            return "";
        }
        uint64_t earliest_finish_time = earliest_start_time + GPU_to_GPU_delay(task_info.input_size) + task_info.expected_execution_timeus;
        allocated_tasks_info[task_name] = {selected_worker_id, earliest_finish_time};
        earliest_available_times[selected_worker_id] = earliest_finish_time;
        if(fetching_model_size > 0 && c_group_available_memory[selected_worker_id] >= fetching_model_size){
            c_group_available_memory[selected_worker_id] -= fetching_model_size;
        }
    }
    std::string allocated_machines;
    for(auto& pathname: tasks_rankings){
        allocated_machines +=  std::to_string(allocated_tasks_info.at(pathname).first) + ",";
    }
    return allocated_machines;
}

template <typename... CascadeTypes>
std::string CascadeContext<CascadeTypes...>::hash_scheduler(std::string entry_prefix, std::string entry_key){
    std::vector<node_id_t> workers_set = this->get_service_client_ref().get_members();
    workers_set.erase(std::remove(workers_set.begin(), workers_set.end(), 0), workers_set.end());
    DataFlowGraph::TaskInfo& entry_task_info = this->prefix_to_task_info[entry_prefix];
    std::vector<std::string>& tasks_rankings = entry_task_info.tasks_rankings_in_dfg;
    std::string allocated_machines;
    for(auto& task_name: tasks_rankings){
        std::string key = task_name + entry_key;
        node_id_t random_node_id = workers_set[std::hash<std::string>{}(key) % workers_set.size()];
        allocated_machines +=  std::to_string(random_node_id) + ",";
    }
    return allocated_machines;
}

template <typename... CascadeTypes>
std::string CascadeContext<CascadeTypes...>::heft_scheduler(std::string entry_prefix){
    std::unordered_map<std::string, std::pair<node_id_t,uint64_t>> allocated_tasks_info;
    std::vector<node_id_t> workers_set = this->get_service_client_ref().get_members();
    workers_set.erase(std::remove(workers_set.begin(), workers_set.end(), 0), workers_set.end());
    node_id_t local_node_id = this->get_service_client_ref().get_my_id();

    auto it = this->prefix_to_task_info.find(entry_prefix);
    if(it == this->prefix_to_task_info.end()){
        dbg_default_error("CascadeContext::tide_scheduler task_info is empty");
        return "";
    }
    DataFlowGraph::TaskInfo& entry_task_info = this->prefix_to_task_info[entry_prefix];

    uint64_t cur_us = get_time_us(true);
    std::vector<std::string>& tasks_rankings = entry_task_info.tasks_rankings_in_dfg;
    std::unordered_map<node_id_t, uint64_t> earliest_available_times;
    for(auto& node_id : workers_set){
        earliest_available_times[node_id] = cur_us;
    }
    for(auto& task_name: tasks_rankings){
        auto& task_info = this->prefix_to_task_info[task_name];
        // 0. PRE-COMPUTE (used later by 2. case1) get the earliest start time, suppose all preq_tasks need to transfer data
        node_id_t selected_worker_id = INVALID_NODE_ID;
	    uint64_t earliest_start_time = UINT64_MAX;
        for(const auto& cur_worker: workers_set){
            uint64_t cur_earliest_start_time = cur_us;
            uint64_t inputs_arrival_time = cur_us;
            if(task_name == tasks_rankings[0] && cur_worker != local_node_id){
                    inputs_arrival_time += CPU_to_CPU_delay(task_info.input_size);
            }
            for(std::string& preq_task_name: task_info.required_objects_pathnames){
                auto& preq_task_info = this->prefix_to_task_info[preq_task_name];
                auto& alloc_info = allocated_tasks_info[preq_task_name];
                uint64_t arrival_time = alloc_info.second;
                if(cur_worker != alloc_info.first){
                    arrival_time += CPU_to_CPU_delay(preq_task_info.output_size);
                }
                inputs_arrival_time = std::max(inputs_arrival_time, arrival_time);
            }
            cur_earliest_start_time = std::max(cur_earliest_start_time, inputs_arrival_time);
            cur_earliest_start_time = std::max(earliest_available_times[cur_worker], cur_earliest_start_time);
            if(cur_earliest_start_time < earliest_start_time){
                earliest_start_time = cur_earliest_start_time;
                selected_worker_id = cur_worker;
            }
        }
        if(selected_worker_id == INVALID_NODE_ID){
            dbg_default_error("CascadeContext::tide_scheduler selected_worker_id == -1");
            return "";
        }
        uint64_t earliest_finish_time = earliest_start_time + GPU_to_GPU_delay(task_info.input_size) + task_info.expected_execution_timeus;
        allocated_tasks_info[task_name] = {selected_worker_id, earliest_finish_time};
        earliest_available_times[selected_worker_id] = earliest_finish_time;
    }
    std::string allocated_machines;
    for(auto& pathname: tasks_rankings){
        allocated_machines +=  std::to_string(allocated_tasks_info.at(pathname).first) + ",";
    }
    return allocated_machines;
}

template <typename... CascadeTypes>
CascadeContext<CascadeTypes...>::~CascadeContext() {
    destroy();
}

}  // namespace cascade
}  // namespace derecho
