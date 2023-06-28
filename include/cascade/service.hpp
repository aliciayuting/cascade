#pragma once
#include <cstdint>
#include <derecho/core/notification.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <typeinfo>
#include <tuple>
#include <derecho/utils/time.h>
#include <list>
#include <condition_variable>
#include <thread>
#include <functional>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <derecho/conf/conf.hpp>
#include "cascade.hpp"
#include "utils.hpp"
#include "object_pool_metadata.hpp"
#include "user_defined_logic_manager.hpp"
#include "data_flow_graph.hpp"
#include "detail/prefix_registry.hpp"

/**
 * The cascade service templates
 *
 * Type neutral templates components go here. Since the server binary and client library has to be type aware (because
 * they are pre-compiled), we separate the api and implementation of them in type-awared header files as follows:
 * - service_types.hpp contains the predefined types for derecho Subgroups, which are specialized from
 *   derecho::cascade::VolatileCascadeStore/PersistentCascadeStore templates.
 * - service_client_api.hpp contains the client API definition.
 * - service_server_api.hpp contains the server API definition. Huh, Server API??? YES! because the application need to
 *   specify their 'onData()' behaviours by implementing the APIs in service_server_api.hpp as a shared library. The
 *   server will load them on restart.
 */
namespace derecho {
namespace cascade {
    /* Cascade Factory type*/
    template <typename CascadeType>
    using Factory = std::function<std::unique_ptr<CascadeType>(persistent::PersistentRegistry*, subgroup_id_t subgroup_id, ICascadeContext*)>;

    /* Cascade Metadata Service type*/
    template<typename...CascadeTypes>
    using CascadeMetadataService = PersistentCascadeStore<
        std::remove_cv_t<std::remove_reference_t<decltype(std::declval<ObjectPoolMetadata<CascadeTypes...>>().get_key_ref())>>,
        ObjectPoolMetadata<CascadeTypes...>,
        &ObjectPoolMetadata<CascadeTypes...>::IK,
        &ObjectPoolMetadata<CascadeTypes...>::IV,
        ST_FILE>;
#define METADATA_SERVICE_SUBGROUP_INDEX (0)


    /* The cascade context to be defined later */
    template <typename... CascadeTypes>
    class CascadeContext;

    /* The Action to be defined later */
    struct Action;
    /**
     * The off-critical data path handler API
     */
    class OffCriticalDataPathObserver: public derecho::DeserializationContext {
    public:
        /**
         * This function has to be re-entrant/thread-safe.
         * @param sender            The sender id
         * @param full_key_string   The full key string
         * @param prefix            The matching prefix length key_string.subtring(0,prefix) returns the prefix.
         *                          Please note that the trailing '/' is included.
         * @param version           The version of the key
         * @param value_ptr         The raw value pointer
         * @param ctxt              The CascadeContext
         * @param worker_id         The off critical data path worker id.
         */
        virtual void operator() (const node_id_t sender,
                                 const std::string& full_key_string,
                                 const uint32_t prefix_length,
                                 persistent::version_t version,
                                 const mutils::ByteRepresentable* const value_ptr,
                                 const std::unordered_map<std::string,bool>& outputs,
                                 ICascadeContext* ctxt,
                                 uint32_t worker_id,
                                 std::string adfg="") = 0;
        /**
         * This function has extra copy overhead of value_ptrs, currently only using it in case of joint prefix in dfg.
         * TODO: optimize this function to avoid extra copy.
         * @param sender            The sender id
         * @param full_key_string   The full key string
         * @param prefix            The matching prefix length key_string.subtring(0,prefix) returns the prefix.
         *                          Please note that the trailing '/' is included.
         * @param version           The version of the key
         * @param value_ptrs        The vector of value shared pointers
         * @param ctxt              The CascadeContext
         * @param worker_id         The off critical data path worker id.
         */
        virtual void operator() (const node_id_t sender,
                                 const std::string& full_key_string,
                                 const uint32_t prefix_length,
                                 persistent::version_t version,
                                 const std::vector<std::shared_ptr<mutils::ByteRepresentable>>& value_ptrs,
                                 const std::unordered_map<std::string,bool>& outputs,
                                 ICascadeContext* ctxt,
                                 uint32_t worker_id,
                                 std::string adfg="") = 0;
    };
    /**
     * Action is an command passed from the on critical data path logic (cascade watcher) to the off critical data path
     * logic, a.k.a. workers, running in the cascade context thread pool.
     *
     * !!! IMPORTANT NOTES ON "ACTION" DESIGN !!!
     * Action carries the key string, version, prefix handler (ocdpo_raw_ptr), and the object value so that the prefix
     * handler has all the information to process in the worker thread. It is important to avoid unnecessary copies
     * because the object value is big sometime (for example, a high resolution video clip). Currently, we copied the
     * value data into a new allocated memory buffer pointed by a unique pointer in the critical data path because the
     * value in critical data path is in Derecho's managed RDMA buffer, which will not last beyond the lifetime of the
     * critical data path. However, even this copy can be avoided using a lock-less design.
     *
     * For example, we can pass the raw pointer to the value in VolatileCascadeStore or PersistentCascadeStore instead of
     * allocating new memory and copying data. But the critical data path keeps updating the value (actually, the old
     * value is removed from the map, and a new value is inserted). Dereferencing the raw pointer might crash with a
     * segmentation fault if the pointed value is reclaimed. Moreover, using lock is not efficient at all because the
     * off critical data path lock will block the critical data path, slowing down the whole system. An optimal solution
     * to this issue is to
     * 1) keep a short history of all the versions in VolatileCascadeStore or PersistentCascadeStore in std::vector<>;
     * 2) enable concurrent access to the value. For example, we can allocate a lock for each of the slot of the history
     * and pass it to the critical data path so that the worker thread can lock the corresponding slot when it is
     * working on that. The number of slots in history should match the size of action buffer.
     *
     * This is a TODO work to be done later. So far, we stick to the extra copy for convenience.
     *
     */
#define ACTION_BUFFER_ENTRY_SIZE    (256)
#define ACTION_BUFFER_SIZE          (8192)
// #define ACTION_BUFFER_SIZE          (1024)
    struct Action {
        node_id_t                       sender;
        std::string                     key_string;
        uint32_t                        prefix_length;
        persistent::version_t           version;
        std::string                     adfg;
        std::shared_ptr<OffCriticalDataPathObserver>   ocdpo_ptr;
        std::vector<std::shared_ptr<mutils::ByteRepresentable>>     value_ptrs;
        std::vector<std::string>        required_object_pathnames;
        std::unordered_map<std::string,bool>           outputs;
        uint64_t                        expected_execution_timeus;
        DataFlowGraph::Statefulness     stateful;
        bool                            is_trigger;
        /**
         * Move constructor
         * @param other     The input Action object
         */
        Action(Action&& other):
            sender(other.sender),
            key_string(other.key_string),
            prefix_length(other.prefix_length),
            version(other.version),
            adfg(other.adfg),
            ocdpo_ptr(std::move(other.ocdpo_ptr)),
            value_ptrs(std::move(other.value_ptrs)),
            required_object_pathnames(other.required_object_pathnames),
            outputs(std::move(other.outputs)),
            expected_execution_timeus(other.expected_execution_timeus),
            stateful(other.stateful),
            is_trigger(other.is_trigger) {}
        /**
         * Constructor
         * @param   _key_string
         * @param   _version
         * @param   _adfg
         * @param   _ocdpo_ptr const reference rvalue
         * @param   _value_ptr
         * @param   _required_object_pathnames
         * @param   _outputs
         * @param   _expected_execution_timeus
         * @param   _stateful
         * @param   _is_trigger
         */
        Action(const node_id_t              _sender = INVALID_NODE_ID,
               const std::string&           _key_string = "",
               const uint32_t               _prefix_length = 0,
               const persistent::version_t& _version = CURRENT_VERSION,
               const std::string&           _adfg = "",
               const std::shared_ptr<OffCriticalDataPathObserver>&  _ocdpo_ptr = nullptr,
               const std::shared_ptr<mutils::ByteRepresentable>&    _value_ptr = nullptr,
               const std::vector<std::string>&    _required_object_pathnames = {},
               const std::unordered_map<std::string,bool>           _outputs = {},
               const uint64_t               _expected_execution_timeus = 0,
               const DataFlowGraph::Statefulness _stateful = DataFlowGraph::Statefulness::STATELESS,
               const bool                   _is_trigger = true):
            sender(_sender),
            key_string(_key_string),
            prefix_length(_prefix_length),
            version(_version),
            adfg(_adfg),
            ocdpo_ptr(_ocdpo_ptr),
            required_object_pathnames(_required_object_pathnames),
            outputs(_outputs),
            expected_execution_timeus(_expected_execution_timeus),
            stateful(_stateful),
            is_trigger(_is_trigger) {
                if(_value_ptr){
                    value_ptrs.emplace_back(_value_ptr);
                }
            }
        Action(const Action&) = delete; // disable copy constructor
        /**
         * Assignment operators
         */
        Action& operator = (Action&&) = default;
        Action& operator = (const Action&) = delete;
        /**
         *  Add value_ptr
         *  @param _value_ptr newly received value add to this action
         *  TODO: check if _value_ptr of this preq has already been added to the value_ptrs
         */
        inline void add_value_ptr(const std::shared_ptr<mutils::ByteRepresentable>& _value_ptr) {
            value_ptrs.emplace_back(_value_ptr);
        }
        inline bool received_all_preq_values() const {
            return (value_ptrs.size() >= required_object_pathnames.size() || adfg.empty());
        }
        /**
         *  fire the action.
         *  @param ctxt
         *  @param worker_id
         */
        inline void fire(ICascadeContext* ctxt,uint32_t worker_id) {
            if (ocdpo_ptr && required_object_pathnames.size() <= 1) {
                dbg_default_trace("In {}: [worker_id={}] action is fired.", __PRETTY_FUNCTION__, worker_id);
                dbg_default_trace("Fired Action name: {}, adfg: {}.", key_string, adfg);
                (*ocdpo_ptr)(sender,key_string,prefix_length,version,value_ptrs.at(0).get(),outputs,ctxt,worker_id, adfg);
            }else if(ocdpo_ptr){
                dbg_default_trace("In {}: [worker_id={}] JOINT-vertex action is fired.", __PRETTY_FUNCTION__, worker_id);
                dbg_default_trace("Fired Action name: {}, adfg: {}.", key_string, adfg);
                (*ocdpo_ptr)(sender,key_string,prefix_length,version,value_ptrs,outputs,ctxt,worker_id, adfg);
            }
        }
        inline explicit operator bool() const {
            return value_ptrs.size() != 0;
        }
    };

    inline std::ostream& operator << (std::ostream& out, const Action& action) {
        out << "ToBeScheduledAction:\n"
            << "\tsender = " << action.sender << "\n"
            << "\tkey = " << action.key_string << "\n"
            << "\tprefix_length = " << action.prefix_length << "\n"
            << "\tversion = " << std::hex << action.version << "\n"
            << "\tocdpo_ptr = " << action.ocdpo_ptr.get() << "\n";
        for (auto& value_ptr:action.value_ptrs) {
            out << "\tvalue_ptr = " << value_ptr.get() << "\n";
        }      
        out << std::endl;
        return out;
    }

    /**
      * Helper data structure used by scheduler
      * TODO: maybe this should be defined at data_flow_graph?
      */
    struct MLModelStats{
        uint64_t model_size;
        uint64_t queueing_requests_count;   // count of requests in the queue that depend on this model
        uint64_t total_requests_count;      // total count of requests that use this model so far
    };

    /**
     * The service will start a cascade service node to serve the client.
     */
    template <typename... CascadeTypes>
    class Service {
        /**
         * Constructor
         * The constructor will load the configuration, start the service thread.
         * Constructor is hidden for singleton.
         *
         * @param dsms deserialization managers
         * @param metadata_service_factory
         * @param factories: subgroup factories.
         */
        Service(const std::vector<DeserializationContext*>& dsms,
                derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_service_factory,
                derecho::cascade::Factory<CascadeTypes>... factories);

    public:
        /**
         * The virtual Service destructor.
         */
        virtual ~Service();
        /**
         * The workhorse
         */
        void run();
        /**
         * Stop the service
         */
        void stop(bool is_joining);
        /**
         * Join the service thread
         */
        void join();
        /**
         * Test if the service is running or stopped.
         */
        bool is_running();
    private:
        /**
         * control synchronization members
         */
        std::mutex service_control_mutex;
        std::condition_variable service_control_cv;
        bool _is_running;
        std::thread service_thread;
        /**
         * The group
         */
        std::unique_ptr<derecho::Group<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>> group;
        /**
         * The CascadeContext
         */
        std::unique_ptr<CascadeContext<CascadeTypes...>> context;

        /**
         * Singleton pointer
         */
        static std::unique_ptr<Service<CascadeTypes...>> service_ptr;

    public:
        /**
         * Start the singleton service
         * Please make sure only one thread call start. We do not defense such an incorrect usage.
         *
         * @param dsms
         * @param metadata_factory - factory for the metadata service.
         * @param factories - the factories to create objects.
         */
        static void start(const std::vector<DeserializationContext*>& dsms,
                          derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_factory,
                          derecho::cascade::Factory<CascadeTypes>... factories);
        /**
         * Check if service is started or not.
         */
        static bool is_started();
        /**
         * shutdown the service
         */
        static void shutdown(bool is_joining=true);
        /**
         * wait on the service util it stop
         */
        static void wait();
    };

    /**
     * Create the critical data path callback function.
     * Application should provide corresponding callbacks. The application MUST hold the ownership of the
     * callback objects and make sure its availability during service lifecycle.
     *
    template <typename KT, typename VT, KT* IK, VT *IV>
    std::shared_ptr<CascadeWatcher<KT,VT,IK,IV>> create_critical_data_path_callback();
     */

    /**
     * defining key strings used in the [CASCADE] section of configuration file.
     */
    #define MIN_NODES_BY_SHARD      "min_nodes_by_shard"
    #define MAX_NODES_BY_SHARD      "max_nodes_by_shard"
    #define DELIVERY_MODES_BY_SHARD "delivery_modes_by_shard"
    #define DELIVERY_MODE_ORDERED   "Ordered"
    #define DELIVERY_MODE_RAW       "Raw"
    #define PROFILES_BY_SHARD       "profiles_by_shard"

    /**
     * The ServiceClient template class contains all APIs needed for read/write data. The four core APIs are put, remove,
     * get, and get_by_time. We also provide a set of helper APIs for the client to get the group topology. By default, the
     * core APIs are talking a random but fix member of the specified subgroup and shard. The client can override this
     * behaviour by specifying other member selection policy (ShardMemberSelectionPolicy).
     *
     * The default policy behaviour depends on the
     */
    enum ShardMemberSelectionPolicy {
        FirstMember,    // use the first member in the list returned from get_shard_members(), this is the default behaviour.
        LastMember,     // use the last member in the list returned from get_shard_members()
        Random,         // use a random member in the shard for each operations(put/remove/get/get_by_time).
        FixedRandom,    // use a random member and stick to that for the following operations.
        RoundRobin,     // use a member in round-robin order.
        KeyHashing,     // use the key's hashing 
        UserSpecified,  // user specify which member to contact.
        InvalidPolicy = -1
    };
    // #define DEFAULT_SHARD_MEMBER_SELECTION_POLICY (ShardMemberSelectionPolicy::FirstMember)
    #define DEFAULT_SHARD_MEMBER_SELECTION_POLICY (ShardMemberSelectionPolicy::RoundRobin)

    template <typename T> struct do_hash {};

    template <> struct do_hash<std::tuple<std::type_index,uint32_t,uint32_t>> {
        size_t operator()(const std::tuple<std::type_index,uint32_t,uint32_t>& t) const {
            return static_cast<size_t>(std::get<0>(t).hash_code() ^ ((std::get<1>(t)<<16) | std::get<2>(t)));
        }
    };


    /** The notification handler type */
    using cascade_notification_handler_t = std::function<void(const Blob&)>;

    /** The CascadeNotificationMessage type */
#define CASCADE_NOTIFICATION_MESSAGE_TYPE   (0x100000000ull)
    struct CascadeNotificationMessage: public mutils::ByteRepresentable {
        /** The object pool pathname, empty string for raw cascade notification message */
        std::string object_pool_pathname;
        /** data */
        Blob blob;

        /** TODO: the default serialization support macro might contain unnecessary copies. Check it!!! */
        DEFAULT_SERIALIZATION_SUPPORT(CascadeNotificationMessage,object_pool_pathname,blob);

        /** constructors */
        CascadeNotificationMessage():
            object_pool_pathname(),
            blob() {}
        CascadeNotificationMessage(CascadeNotificationMessage&& other):
            object_pool_pathname(other.object_pool_pathname),
            blob(std::move(other.blob)) {}
        CascadeNotificationMessage(const CascadeNotificationMessage& other):
            object_pool_pathname(other.object_pool_pathname),
            blob(other.blob) {}
        CascadeNotificationMessage(const std::string& _object_pool_pathname,
                const Blob& _blob) :
            object_pool_pathname(_object_pool_pathname),
            blob(_blob) {}
    };

    /**
     * This is the structure for the server side notification handlers
     */
    template <typename SubgroupType>
    struct SubgroupNotificationHandler {
        // key: object_pool_pathname
        // value: an option for the handler
        // The handler for "" key is the default handler, which will always be triggered.
        std::unordered_map<std::string, std::optional<cascade_notification_handler_t>> object_pool_notification_handlers;
        mutable std::unique_ptr<std::mutex> object_pool_notification_handlers_mutex;

        SubgroupNotificationHandler():
            object_pool_notification_handlers_mutex(std::make_unique<std::mutex>()) {}

        template <typename T>
        inline void initialize(derecho::ExternalClientCaller<SubgroupType,T>& subgroup_caller) {
            dbg_default_trace("SubgroupNotificationHandler(this={:x}) is initialized for SubgroupType:{}",
                    reinterpret_cast<uint64_t>(this),typeid(SubgroupType).name());
            subgroup_caller.register_notification_handler(
                    [this](const derecho::NotificationMessage& msg){
                        dbg_default_trace("subgroup notification handler is triggered with this={:x}, msg type={}, size={} bytes",
                                reinterpret_cast<uint64_t>(this), msg.message_type, msg.size);
                        (*this)(msg);
                    });
        }

        inline void operator ()(const derecho::NotificationMessage& msg) {
            dbg_default_trace("SubgroupNotificationHandler(this={:x}) is triggered with message_type={:x}, size={} bytes",
                    reinterpret_cast<uint64_t>(this),msg.message_type, msg.size);
            if (msg.message_type != CASCADE_NOTIFICATION_MESSAGE_TYPE) {
                return;
            }
            // mutils::deserialize_and_run<CascadeNotificationMessage>(nullptr, msg.body,
            mutils::deserialize_and_run(nullptr, msg.body,
                    [this](const CascadeNotificationMessage& cascade_message)->void {
                        dbg_default_trace("Handling cascade_message: {}. size={} bytes",
                                cascade_message.object_pool_pathname,cascade_message.blob.size);
                        std::lock_guard<std::mutex> lck(*object_pool_notification_handlers_mutex);
                        // call default handler
                        if (object_pool_notification_handlers.find("") !=
                            object_pool_notification_handlers.cend()) {
                            if (object_pool_notification_handlers.at("").has_value()) {
                                (*object_pool_notification_handlers.at(""))(cascade_message.blob);
                            }
                        }
                        // call object pool handler
                        if (object_pool_notification_handlers.find(cascade_message.object_pool_pathname) !=
                            object_pool_notification_handlers.cend()) {
                            if (object_pool_notification_handlers.at(cascade_message.object_pool_pathname).has_value()) {
                                (*object_pool_notification_handlers.at(cascade_message.object_pool_pathname))(cascade_message.blob);
                            }
                        }
                    });
        }
    };

    template <typename SubgroupType>
    using per_type_notification_handler_registry_t =
        std::unordered_map<uint32_t,SubgroupNotificationHandler<SubgroupType>>;

    template <typename... CascadeTypes>
    class ServiceClient {
    private:
        // default caller as an external client.
        std::unique_ptr<derecho::ExternalGroupClient<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>> external_group_ptr;
        mutable std::mutex external_group_ptr_mutex;
        // caller as a group member.
        derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* group_ptr;
        mutable std::mutex group_ptr_mutex;
        // cascade server side notification handler registry.
        mutable mutils::KindMap<per_type_notification_handler_registry_t,CascadeTypes...> notification_handler_registry;
        mutable std::mutex notification_handler_registry_mutex;
        /**
         * 'member_selection_policies' is a map from derecho shard to its member selection policy.
         * We use a 3-tuple consisting of subgroup type index, subgroup index, and shard index to identify a shard. And
         * the policy is defined by a 2-tuple with the ShardMemberSelectionPolicy enum and a user specified node id, in
         * case of ShardMemorySelectionPolicy::UserSpecified. The user specified node id is used as member index if the
         * policy is ShardMemberSelectionPolicy::RoundRobin
         *
         * The default member selection policy is defined as SHARD_MEMBER_SELECTION_POLICY (ShardMemberSelectionPolicy::FirstMember).
         */
        std::unordered_map<
            std::tuple<std::type_index,uint32_t,uint32_t>,
            std::tuple<ShardMemberSelectionPolicy,node_id_t>,
            do_hash<std::tuple<std::type_index,uint32_t,uint32_t>>> member_selection_policies;
        mutable std::shared_mutex member_selection_policies_mutex;
        /**
         * 'member_cache' is a map from derecho shard to its member list. This cache is used to accelerate the member
         * choices process. If the client cannot connect to the cached member (after a couple of retries), it will refresh
         * the corresponding cache entry.
         */
        std::unordered_map<
            std::tuple<std::type_index,uint32_t,uint32_t>,
            std::vector<node_id_t>,
            do_hash<std::tuple<std::type_index,uint32_t,uint32_t>>> member_cache;
        mutable std::shared_mutex member_cache_mutex;
        /**
         * 'object_pool_info_cache' is a local cache for object pool metadata. This cache is used to accelerate the
         * object access process. If an object pool does not exists, it will be loaded from metadata service.
         */
        std::unordered_map<
            std::string,
            ObjectPoolMetadata<CascadeTypes...>> object_pool_metadata_cache;
        mutable std::shared_mutex object_pool_metadata_cache_mutex;

        /**
         * Pick a member by a given a policy.
         * @param subgroup_index
         * @param shard_index
         * @param key_for_hashing   - only for KeyHashing policy, ignored otherwise.
         * @param retry             - if true, refresh the member_cache.
         */
        template <typename SubgroupType, typename KeyTypeForHashing>
        node_id_t pick_member_by_policy(uint32_t subgroup_index,
                                        uint32_t shard_index,
                                        const KeyTypeForHashing& key_for_hashing,
                                        bool retry = false);

        /**
         * Refresh(or fill) a member cache entry.
         * @param subgroup_index
         * @param shard_index
         */
        template <typename SubgroupType>
        void refresh_member_cache_entry(uint32_t subgroup_index, uint32_t shard_index);

        /**
         * Helper function to fill entire member_cache, initialized upon server start. 
         * Used by single_node_trigger_put by scheduler
        */
        template <typename FirstType, typename SecondType, typename... RestTypes>
        void type_recursive_refresh_member_cache();

        /**
         * type_recursive_refresh_member_cache() base case
        */
        template <typename LastType>
        void type_recursive_refresh_member_cache();

        /**
         * Deprecated: Please use key_to_shard() instead
         *
         * Metadata API Helper: turn a string key to subgroup index and shard index
         *
        template <typename SubgroupType>
        std::pair<uint32_t,uint32_t> key_to_subgroup_index_and_shard_index(const typename SubgroupType::KeyType& key,
                bool check_object_location = true);
         */

        /**
         * Metadata API Helper: turn a string key to subgroup type index, subgroup index, and shard index.
         */
        template <typename KeyType>
        std::tuple<uint32_t,uint32_t,uint32_t> key_to_shard(
                const KeyType& key, bool check_object_location = true);

        /**
         *
         * Scheduler helper: turn a node id to subgroup type index, subgroup index, and shard index.
         */
        std::tuple<std::type_index,uint32_t> node_id_to_subgroup(
                node_id_t node_id);
        

    public:
        /**
         * The Constructor
         * We prevent calling the constructor explicitely, because the ServiceClient is a singleton.
         * @param _group_ptr The caller can pass a pointer pointing to a derecho group object. If the pointer is
         *                   valid, the implementation will reply on the group object instead of creating an external
         *                   client to communicate with group members.
         */
        ServiceClient(derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* _group_ptr=nullptr);

        /**
         * ServiceClient can be an external client or a cascade server. is_external_client() test this condition.
         * The external client implementation is based on ExternalGroupClient<> while the cascade node implementation is
         * based on Group<>.
         *
         * @return true for external client; other wise false.
         */
        inline bool is_external_client() const;

        /**
         * Derecho group helpers: They derive the API in derecho::ExternalClient.
         * - get_my_id                  return my local node id.
         * - get_members                returns all members in the top-level Derecho group.
         * - get_subgroup_members       returns a vector of vectors of node ids: [[node ids in shard 0],[node ids in shard 1],...]
         * - get_shard_members          returns the members in a shard specified by subgroup id(or subgroup type/index pair) and
         *                              shard index.
         * - get_number_of_subgroups    returns the number of subgroups of a given type
         * - get_number_of_shards       returns the number of shards of a given subgroup
         * During view change, the Client might experience failure if the member is gone. In such a case, the client needs
         * refresh its local member cache by calling get_shard_members.
         */
        node_id_t get_my_id() const;

        std::vector<node_id_t> get_members() const;

        template <typename SubgroupType>
        std::vector<std::vector<node_id_t>> get_subgroup_members(uint32_t subgroup_index) const;
    protected:
        template <typename FirstType,typename SecondType, typename...RestTypes>
        std::vector<std::vector<node_id_t>> type_recursive_get_subgroup_members(uint32_t type_index, uint32_t subgroup_index) const;
        template <typename LastType>
        std::vector<std::vector<node_id_t>> type_recursive_get_subgroup_members(uint32_t type_index, uint32_t subgroup_index) const;
    public:
        std::vector<std::vector<node_id_t>> get_subgroup_members(const std::string& object_pool_pathname);

        template <typename SubgroupType>
        std::vector<node_id_t> get_shard_members(uint32_t subgroup_index,uint32_t shard_index) const;
    protected:
        template <typename FirstType,typename SecondType, typename...RestTypes>
        std::vector<node_id_t> type_recursive_get_shard_members(uint32_t type_index,
                uint32_t subgroup_index, uint32_t shard_index) const;
        template <typename LastType>
        std::vector<node_id_t> type_recursive_get_shard_members(uint32_t type_index,
                uint32_t subgroup_index, uint32_t shard_index) const;
    public:
        std::vector<node_id_t> get_shard_members(const std::string& object_pool_pathname,uint32_t shard_index);

        template <typename SubgroupType>
        uint32_t get_number_of_subgroups() const;

        template <typename SubgroupType>
        uint32_t get_number_of_shards(uint32_t subgroup_index) const;

        // type recursive helpers for get_number_of_shards
    protected:
        template <typename FirstType,typename SecondType, typename...RestTypes>
        uint32_t type_recursive_get_number_of_shards(uint32_t type_index, uint32_t subgroup_index) const;
        template <typename LastType>
        uint32_t type_recursive_get_number_of_shards(uint32_t type_index, uint32_t subgroup_index) const;
    public:
        /**
         * This get_number_of_shards() overload the typed version.
         * @param subgroup_type_index   - the type index of the subrgoup type.
         * @param subgroup_index        - the subgroup index in the given type.
         */
        uint32_t get_number_of_shards(uint32_t subgroup_type_index, uint32_t subgroup_index) const;

        /**
         * This get_number_of_shards(), pick subgroup using object pool pathname.
         * @param object_pool_pathname  - the object pool name
         */
        uint32_t get_number_of_shards(const std::string& object_pool_pathname);

        /**
         * Member selection policy control API.
         * - set_member_selection_policy updates the member selection policies.
         * - get_member_selection_policy read the member selection policies.
         * @param subgroup_index
         * @param shard_index
         * @param policy
         * @param user_specified_node_id
         * @return get_member_selection_policy returns a 2-tuple of policy and user_specified_node_id.
         */
        template <typename SubgroupType>
        void set_member_selection_policy(uint32_t subgroup_index,uint32_t shard_index,
                ShardMemberSelectionPolicy policy,node_id_t user_specified_node_id=INVALID_NODE_ID);

        template <typename SubgroupType>
        std::tuple<ShardMemberSelectionPolicy,node_id_t> get_member_selection_policy(
                uint32_t subgroup_index, uint32_t shard_index) const;

        /**
         * "put" writes an object to a given subgroup/shard.
         *
         * @param object            the object to write.
         *                          User provided SubgroupType::ObjectType must have the following two members:
         *                          - SubgroupType::ObjectType::key of SubgroupType::KeyType, which must be set to a
         *                            valid key.
         *                          - SubgroupType::ObjectType::ver of std::tuple<persistent::version_t, uint64_t>.
         *                            Similar to the return object, this member is a two tuple with the first member
         *                            for a version and the second for a timestamp. A caller of put can specify either
         *                            of the version and timestamp meaning what is the latest version/timestamp the caller
         *                            has seen. Cascade will reject the write if the corresponding key has been updated
         *                            already. TODO: should we make it an optional feature?
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the version and timestamp of the put operation.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> put(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index, uint32_t shard_index);
        /**
         * "type_recursive_put" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. And the FirstType,
         *                          SecondType, ..., RestTypes should be in the same order.
         * @object                  the object to write
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         *
         * @return a future to the version and timestamp of the put operation.
         */
    protected:
        template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> type_recursive_put(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename ObjectType, typename LastType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> type_recursive_put(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        /**
         * object pool version
         * @param object            the object to write, the object pool is extracted from the object key.
         *
         * @return a future to the version and timestamp of the put operation.
         */
        template <typename ObjectType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> put(const ObjectType& object);

        /**
         * "put_and_forget" writes an object to a given subgroup/shard, but no return value.
         *
         * @param object            the object to write.
         *                          User provided SubgroupType::ObjectType must have the following two members:
         *                          - SubgroupType::ObjectType::key of SubgroupType::KeyType, which must be set to a
         *                            valid key.
         *                          - SubgroupType::ObjectType::ver of std::tuple<persistent::version_t, uint64_t>.
         *                            Similar to the return object, this member is a two tuple with the first member
         *                            for a version and the second for a timestamp. A caller of put can specify either
         *                            of the version and timestamp meaning what is the latest version/timestamp the caller
         *                            has seen. Cascade will reject the write if the corresponding key has been updated
         *                            already. TODO: should we make it an optional feature?
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         */
        template <typename SubgroupType>
        void put_and_forget(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_put_and_forget" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @object                  the object to write
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         */
    protected:
        template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
        void type_recursive_put_and_forget(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename ObjectType, typename LastType>
        void type_recursive_put_and_forget(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        /**
         * object pool version
         * @param object    the object to write, the object pool is extracted from the object key.
         */
        template <typename ObjectType>
        void put_and_forget(const ObjectType& object);

        /**
         * "trigger_put" writes an object to a given subgroup/shard.
         *
         * @param object            the object to write.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a void future.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<void> trigger_put(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index, uint32_t shard_index);

    protected:
        /**
         * "type_recursive_trigger_put" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @object                  the object to write
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         */
        template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<void> type_recursive_trigger_put(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename ObjectType, typename LastType>
        derecho::rpc::QueryResults<void> type_recursive_trigger_put(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        /**
         * object pool version
         * @param object    the object to write, the object pool is extracted from the object key.
         */
        template <typename ObjectType>
        derecho::rpc::QueryResults<void> trigger_put(const ObjectType& object);
        /**
         * "collective_trigger_put" writes an object to a set of nodes.
         *
         * Please notice that returning from QueryResults<void>::get() only means that the message has been sent by the
         * sender. It does NOT guarantee that the message is/will be successfully processed by the remote side. However,
         * we agree that QueryResults<void> should reflect exceptions or errors either on local or remote side, which is
         * not enabled so far. TODO: Track exception in derecho::rpc::QueryResults<void>
         *
         * @param object            the object to write.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param nodes             node ids for the set of nodes.
         *
         * @return an array of void futures, which length is nodes.size()
         */
        template <typename SubgroupType>
        void collective_trigger_put(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index,
                std::unordered_map<node_id_t,std::unique_ptr<derecho::rpc::QueryResults<void>>>& nodes_and_futures);
    
    protected:
        /**
         * "type_recursive_single_trigger_put" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @object                  the object to write
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @node_id                 the node_id of object to write to
         */
        template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
        void type_recursive_single_node_trigger_put(
                std::type_index type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                node_id_t node_id,
                uint32_t round);

        template <typename ObjectType, typename LastType>
        void type_recursive_single_node_trigger_put(
                std::type_index type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                node_id_t node_id,
                uint32_t round);
        /**
         * "single_node_trigger_put" writes an object to a specific node. Only for internal use by scheduler.
         *  
         * Similar logic as collective_trigger_put, omitted the loop for set of nodes 
         *
         * @param object            the object to write.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param node_id           node_id that p2p trigger_put to
         */
        template <typename SubgroupType>
        void single_node_trigger_put(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index,
                node_id_t node_id);
    public:
        /**
         * "single_node_trigger_put" writes an object to a specific node. 
         *  This function is supposed to be internally use by emit function to send tasks to scheduled gpu task execution
         *
         * @param object            the object to write.
         * @param node_id           node_id that p2p trigger_put to
         */
        template <typename ObjectType>
        void single_node_trigger_put(const ObjectType object, node_id_t node_id);

        /**
         * "remove" deletes an object with the given key.
         *
         * @param key               the object key
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the version and timestamp of the put operation.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove(const typename SubgroupType::KeyType& key,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_remove" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @key                     the key
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         *
         * @return a future to the version and timestamp of the put operation.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> type_recursive_remove(
                uint32_t type_index,
                const KeyType& key,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> type_recursive_remove(
                uint32_t type_index,
                const KeyType& key,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        /**
         * object pool version
         */
        template <typename KeyType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove(const KeyType& key);

        /**
         * "get" retrieve the object of a given key
         *
         * @param key               the object key
         * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
         *                          state of the key. Otherwise, it will try to read the key's state at version.
         * @param stable            if true, get only report the version whose persistent data is safe, meaning the
         *                          persistent data is persisted on all replicas.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> get(
                const typename SubgroupType::KeyType& key,
                const persistent::version_t& version = CURRENT_VERSION,
                bool stable = true,
                uint32_t subgroup_index = 0,
                uint32_t shard_index = 0);
        /**
         * "type_recursive_get" is a helper function for internal use only.
         * @param type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @param key               the key
         * @param version           the version
         * @param stable            stable or not?
         * @param subgroup_index    the subgroup index in the subgroup type designated by type_index
         * @param shard_index       the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_get(
                uint32_t type_index,
                const KeyType& key,
                const persistent::version_t& version,
                bool stable,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        auto type_recursive_get(
                uint32_t type_index,
                const KeyType& key,
                const persistent::version_t& version,
                bool stable,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        /**
         * object pool version
         */
        template <typename KeyType>
        auto get(
                const KeyType& key,
                const persistent::version_t& version = CURRENT_VERSION,
                bool stable = true);

        /**
         * "multi_get" retrieve the object of a given key, this operation involves atomic broadcast
         *
         * @param key               the object key
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> multi_get(const typename SubgroupType::KeyType& key,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_multi_get"   is a helper function for internal use only.
         * @type_index                  the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                              SecondType, .../ RestTypes should be in the same order.
         * @key                         the key
         * @subgroup_index              the subgroup index in the subgroup type designated by type_index
         * @shard_index                 the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_multi_get(
                uint32_t type_index,
                const KeyType& key,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        auto type_recursive_multi_get(
                uint32_t type_index,
                const KeyType& key,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:

        /**
         * object pool version
         */
        template <typename KeyType>
        auto multi_get(const KeyType& key);

        /**
         * "get_by_time" retrieve the object of a given key
         *
         * @param key               the object key
         * @param ts_us             Wall clock time in microseconds.
         * @param stable            stable get or not
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> get_by_time(
                const typename SubgroupType::KeyType& key,
                const uint64_t& ts_us,
                const bool stable = true,
                uint32_t subgroup_index = 0,
                uint32_t shard_index = 0);

        /**
         * "type_recursive_get_by_time" is a helper function for internal use only.
         * @param type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @param key               the key
         * @param ts_us             Wall clock time in microseconds.
         * @param stable            stable get or not
         * @param subgroup_index    the subgroup index in the subgroup type designated by type_index
         * @param shard_index       the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_get_by_time(
                uint32_t type_index,
                const KeyType& key,
                const uint64_t& ts_us,
                const bool stable,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        auto type_recursive_get_by_time(
                uint32_t type_index,
                const KeyType& key,
                const uint64_t& ts_us,
                const bool stable,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:

        /**
         * object pool version
         */
        template <typename KeyType>
        auto get_by_time(
                const KeyType& key,
                const uint64_t& ts_us,
                const bool stable = true);

        /**
         * "get_size" retrieve size of the object of a given key
         *
         * @param key               the object key
         * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
         *                          state of the key. Otherwise, it will try to read the key's state at version.
         * @param stable            stable get or not
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved size.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<uint64_t> get_size(
                const typename SubgroupType::KeyType& key,
                const persistent::version_t& version,
                const bool stable = true,
                uint32_t subgroup_index = 0,
                uint32_t shard_index = 0);

        /**
         * "type_recursive_get_size" is a helper function for internal use only.
         * @param type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @param key               the key
         * @param version           version
         * @param stable            stable get size or not
         * @param subgroup_index    the subgroup index in the subgroup type designated by type_index
         * @param shard_index       the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<uint64_t> type_recursive_get_size(
                uint32_t type_index,
                const KeyType& key,
                const persistent::version_t& version,
                const bool stable,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        derecho::rpc::QueryResults<uint64_t> type_recursive_get_size(
                uint32_t type_index,
                const KeyType& key,
                const persistent::version_t& version,
                const bool stable,
                uint32_t subgroup_index,
                uint32_t shard_index);

    public:
        /**
         * object pool version
         */
        template <typename KeyType>
        derecho::rpc::QueryResults<uint64_t> get_size(
                const KeyType& key,
                const persistent::version_t& version,
                const bool stable = true);

        /**
         * "multi_get_size" retrieve size of the object of a given key
         *
         * @param key               the object key
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved size.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<uint64_t> multi_get_size(
                const typename SubgroupType::KeyType& key,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_multi_get_size" is a helper function for internal use only.
         * @param type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @param key               the key
         * @param subgroup_index    the subgroup index in the subgroup type designated by type_index
         * @param shard_index       the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<uint64_t> type_recursive_multi_get_size(
                uint32_t type_index,
                const KeyType& key,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        derecho::rpc::QueryResults<uint64_t> type_recursive_multi_get_size(
                uint32_t type_index,
                const KeyType& key,
                uint32_t subgroup_index,
                uint32_t shard_index);

    public:
        /**
         * object pool version
         */
        template <typename KeyType>
        derecho::rpc::QueryResults<uint64_t> multi_get_size(const KeyType& key);

        /**
         * "get_size_by_time" retrieve size of the object of a given key
         *
         * @param key               the object key
         * @param ts_us             Wall clock time in microseconds.
         * @param stable            stable get or not
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved size.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<uint64_t> get_size_by_time(
                const typename SubgroupType::KeyType& key,
                const uint64_t& ts_us,
                const bool stable = true,
                uint32_t subgroup_index = 0,
                uint32_t shard_index = 0);

        /**
         * "type_recursive_get_size" is a helper function for internal use only.
         * @param type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @param key               the key
         * @param ts_us             Wall clock time in microseconds.
         * @param stable            stable get or not
         * @param subgroup_index    the subgroup index in the subgroup type designated by type_index
         * @param shard_index       the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<uint64_t> type_recursive_get_size_by_time(
                uint32_t type_index,
                const KeyType& key,
                const uint64_t& ts_us,
                const bool stable,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        derecho::rpc::QueryResults<uint64_t> type_recursive_get_size_by_time(
                uint32_t type_index,
                const KeyType& key,
                const uint64_t& ts_us,
                const bool stable,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:

        /**
         * object pool version
         */
        template <typename KeyType>
        derecho::rpc::QueryResults<uint64_t> get_size_by_time(
                const KeyType& key,
                const uint64_t& ts_us,
                const bool stable = true);

        /**
         * "list_keys" retrieve the list of keys in a shard
         *
         * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
         *                          state of the key. Otherwise, it will try to read the key's state at version.
         * @param stable            stable or not
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> list_keys(
                const persistent::version_t& version,
                const bool stable = true,
                uint32_t subgroup_index = 0,
                uint32_t shard_index = 0);

    protected:
        template <typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_list_keys(
                uint32_t type_index,
                const persistent::version_t& version,
                const bool stable,
                const std::string& object_pool_pathname);
        template <typename LastType>
        auto type_recursive_list_keys(
                uint32_t type_index,
                const persistent::version_t& version,
                const bool stable,
                const std::string& object_pool_pathname);
        template <typename SubgroupType>
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>>
            __list_keys(const persistent::version_t& version, const bool stable, const std::string& object_pool_pathname);
    public:
        /**
         * object pool version
         * @param version               if version is
         * @param object_pool_pathname  the object pathname
         */
        auto list_keys(const persistent::version_t& version, const bool stable, const std::string& object_pool_pathname);

        template <typename KeyType>
        std::vector<KeyType> wait_list_keys(
                                std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<KeyType>>>>& future);

        /**
         * "multi_list_keys" retrieve the list of keys in a shard
         *
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> multi_list_keys(
                uint32_t subgroup_index,
                uint32_t shard_index);

    protected:
        template <typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_multi_list_keys(
                uint32_t type_index,
                const std::string& object_pool_pathname);
        template <typename LastType>
        auto type_recursive_multi_list_keys(
                uint32_t type_index,
                const std::string& object_pool_pathname);
        template <typename SubgroupType>
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>>
            __multi_list_keys(const std::string& object_pool_pathname);
    public:
        /**
         * object pool version
         * @param object_pool_pathname  the object pathname
         */
        auto multi_list_keys(const std::string& object_pool_pathname);

        /**
         * "list_keys_by_time" retrieve the list of keys in a shard
         *
         * @param ts_us             Wall clock time in microseconds.
         * @param stable
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> list_keys_by_time(
                const uint64_t& ts_us,
                const bool stable = true,
                uint32_t subgroup_index = 0,
                uint32_t shard_index = 0);

    protected:
        template <typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_list_keys_by_time(
                uint32_t type_index,
                const uint64_t& ts_us,
                const bool stable,
                const std::string& object_pool_pathname);
        template <typename LastType>
        auto type_recursive_list_keys_by_time(
                uint32_t type_index,
                const uint64_t& ts_us,
                const bool stable,
                const std::string& object_pool_pathname);
        template <typename SubgroupType>
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>>
            __list_keys_by_time(const uint64_t& ts_us, const bool stable, const std::string& object_pool_pathname);
    public:
        /**
        * object pool version
        * @param ts_us                  timestamp
        * @param object_pool_pathname   the object pathname
        */
        auto list_keys_by_time(const uint64_t& ts_us, const bool stable, const std::string& object_pool_pathname);

        /**
         * Object Pool Management API: refresh object pool cache
         */
        void refresh_object_pool_metadata_cache();

        /**
         * Object Pool Management API: create object pool
         *
         * @tparam SubgroupType     Type of the subgroup for the created object pool
         * @param  pathname         Object pool's pathname as identifier.
         * @param  subgroup_index   Index of the subgroup
         * @param  sharding_policy  The default sharding policy for this object pool
         * @param  object_locations The set of special object locations.
         *
         * @return a future to the version and timestamp of the put operation.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> create_object_pool(
                const std::string& pathname, const uint32_t subgroup_index,
                const sharding_policy_t sharding_policy = HASH, const std::unordered_map<std::string,uint32_t>& object_locations = {});

        /**
         * ObjectPoolManagement API: remote object pool
         *
         * @param  pathname         Object pool pathname
         *
         * @return a future to the version and timestamp of the put operation.
         */
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove_object_pool(const std::string& pathname);

        /**
         * ObjectPoolManagement API: find object pool
         *
         * @param  pathname         Object pool pathname
         *
         * @return the object pool metadata
         */
        ObjectPoolMetadata<CascadeTypes...> find_object_pool(const std::string& pathname);

        /**
         * ObjectPoolManagement API: list all the object pools by pathnames
         *
         * @param refresh           false for cached object ids, true for refreshed ids.
         *
         * @return the pool ids.
         */
        std::vector<std::string> list_object_pools(bool refresh = false);

        /**
         * Register an notification handler to a subgroup. If such a handler has been registered, it will be replaced
         * by the new one.
         *
         * @tparam SubgroupType     The Subgroup Type
         * @param handler           The handler to reigster
         * @param subgroup_index    Index of the subgroup
         *
         * @return true if a previous notification handler is replaced.
         */
        template <typename SubgroupType>
        bool register_notification_handler(
                const cascade_notification_handler_t& handler,
                const uint32_t subgroup_index = 0);

    protected:
        template <typename SubgroupType>
        bool register_notification_handler(
                const cascade_notification_handler_t& handler,
                const std::string& object_pool_pathname,
                const uint32_t subgroup_index);
        template <typename FirstType,typename SecondType, typename...RestTypes>
        bool type_recursive_register_notification_handler(
                uint32_t type_index,
                const cascade_notification_handler_t& handler,
                const std::string& object_pool_pathname,
                const uint32_t subgroup_index);
        template <typename LastType>
        bool type_recursive_register_notification_handler(
                uint32_t type_index,
                const cascade_notification_handler_t& handler,
                const std::string& object_pool_pathname,
                const uint32_t subgroup_index);

    public:
        /**
         * Register notification handler(object pool version). If such a handler has been registered, it will be
         * replaced by the new one.
         *
         * @tparam SubgroupType         The Subgroup Type
         * @param handler               The handler to reigster
         * @param object_pool_pathname  To with object pool is this handler registered.
         *
         * @return true if a previous notification handler is replaced.
         */
        bool register_notification_handler(
                const cascade_notification_handler_t& handler,
                const std::string& object_pool_pathname);

        /**
         * Send a notification message to an external client.
         *
         * @tparam SubgroupType     The Subgroup Type
         * @param msg               The message to send
         * @param subgroup_index    The subgroup index
         * @param client_id         The node id of the external client to be notified
         */
        template <typename SubgroupType>
        void notify(const Blob& msg,
                const uint32_t subgroup_index,
                const node_id_t client_id) const;
    protected:
        template <typename SubgroupType>
        void notify(const Blob& msg,
                const std::string& object_pool_pathname,
                const uint32_t subgroup_index,
                const node_id_t client_id) const;
        template <typename FirstType, typename SecondType, typename... RestTypes>
        void type_recursive_notify(
                uint32_t type_index,
                const Blob& msg,
                const std::string& object_pool_pathname,
                const uint32_t subgroup_index,
                const node_id_t client_id) const;
        template <typename LastType>
        void type_recursive_notify(
                uint32_t type_index,
                const Blob& msg,
                const std::string& object_pool_pathname,
                const uint32_t subgroup_index,
                const node_id_t client_id) const;
    public:
        /**
         * Send a notification message to an external client.
         *
         * @param msg                   The messgae to send
         * @param object_pool_pathname  In which object_pool the notification is in.
         * @param client_id             The client id
         */
        void notify(const Blob& msg,
                const std::string& object_pool_pathname,
                const node_id_t client_id);

#ifdef ENABLE_EVALUATION
        /**
         * Dump the timestamp log entries into a file on each of the nodes in a shard.
         *
         * @param filename         - the output filename
         * @param subgroup_index   - the subgroup index
         * @param shard_index      - the shard index
         *
         * @return query results
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<void> dump_timestamp(const std::string& filename, const uint32_t subgroup_index, const uint32_t shard_index);

        /**
         * The object store version:
         *
         * @param filename             -   the filename
         * @param object_pool_pathname -   the object pool pathname
         */
        void dump_timestamp(const std::string& filename, const std::string& object_pool_pathname);

        /**
         * Dump the timestamp log entries into a file on each of the nodes in a subgroup.
         *
         * @param filename         - the output filename
         * @param subgroup_index   - the subgroup index
         */
        template <typename SubgroupType>
        void dump_timestamp(const uint32_t subgroup_index, const std::string& filename);

    protected:
        template <typename FirstType, typename SecondType, typename... RestTypes>
        void type_recursive_dump(uint32_t type_index, uint32_t subgroup_index, const std::string& filename);
        
        template <typename LastType>
        void type_recursive_dump(uint32_t type_index, uint32_t subgroup_index, const std::string& filename);

    public:
#ifdef DUMP_TIMESTAMP_WORKAROUND
        /**
         * Dump the timestamp log entries into a file on a specific node.
         *
         * @param filename         - the output filename
         * @param subgroup_index   - the subgroup index
         * @param shard_index      - the shard index
         * @param node_id          - the given node id.
         *
         * @return a vector of query results.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<void> dump_timestamp_workaround(const std::string& filename, const uint32_t subgroup_index, const uint32_t shard_index, const node_id_t node_id);
#endif

        /**
         * Evaluate the ordered put performance inside a shard. Please note that those put does not involve the
         * external client data path.
         *
         * @param message_size      - the message size for the shard. TODO: we should be able to retrieve the maximum
         *                            message size from SubgroupType, subgroup_index and shard_index. How?
         * @param duration_sec      - the duration of the test in seconds.
         * @param subgroup_index    - the subgroup index
         * @param shard_index       - the shard index
         *
         * @return the value in ops.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<double> perf_put(const uint32_t message_size, const uint64_t duration_sec, const uint32_t subgroup_index, const uint32_t shard_index);
#endif//ENABLE_EVALUATION

        const static std::vector<std::type_index> subgroup_type_order;
        const static uint32_t invalid_subgroup_type_index;
        /**
         * Get type index
         * @return the the subgroup type index
         */
        template <typename SubgroupType>
        inline static uint32_t get_subgroup_type_index();

        /**
         * Helper for scheduler accessing derechoSST
         * Get the updated cached_models_info of all nodes in the group from derechoSST
         * @param  _group_cached_models_info     CascadeContext cached group's gpu_models information to update
        */
        void get_updated_group_cached_models_info(
                                            std::unordered_map<node_id_t, std::set<uint32_t>>& _group_cached_models_info,
                                            std::unordered_map<node_id_t, uint64_t>& _group_available_memory,
                                            const std::unordered_map<uint64_t, MLModelStats>&   _local_ml_models_stats);

        /**
         * Get the updated load_info of all nodes in the group from derechoSST
         * @param  _group_queue_wait_times     CascadeContext cached group's queue_wait_time information to update
        */
        void get_updated_group_queue_wait_times(std::unordered_map<node_id_t, uint64_t>& _group_queue_wait_times);

        /**
         * Send the local updated cached_models_info to all nodes in the group, via derechoSST
         * @param  _local_group_models     local cached_models_info information from CascadeContext 
        */
        void send_local_cached_models_info_to_group(std::set<uint32_t> _local_group_models);

        /**
         * Send the local updated queue_wait_time to all nodes in the group, via derechoSST
         * @param  _local_queue_wait_time     local queue_wait_time information from CascadeContext 
        */
        void send_local_queue_wait_time(uint64_t _local_queue_wait_time);

        /* singleton */
    private:
        static std::unique_ptr<ServiceClient> service_client_singleton_ptr;
        static std::mutex                     singleton_mutex;
    public:
        /**
         * Initialize the service_client_single_ptr singleton with a cascade service. This can only be called once
         * before any get_service_client() is called.
         * @param _group_ptr The caller can pass a pointer pointing to a derecho group object. If the pointer is
         *                   valid, the implementation will reply on the group object instead of creating an external
         *                   client to communicate with group members.
         */
        static void initialize(derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* _group_ptr);

        /**
         * Get the singleton ServiceClient API. If it does not exists, initialize it as an external client.
         */
        static ServiceClient& get_service_client();
    }; // ServiceClient


    /**
     * configuration keys
     */
    #define CASCADE_CONTEXT_NUM_STATELESS_WORKERS_MULTICAST   "CASCADE/num_stateless_workers_for_multicast_ocdp"
    #define CASCADE_CONTEXT_NUM_STATELESS_WORKERS_P2P         "CASCADE/num_stateless_workers_for_p2p_ocdp"
    #define CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_MULTICAST   "CASCADE/num_stateful_workers_for_multicast_ocdp"
    #define CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_P2P         "CASCADE/num_stateful_workers_for_p2p_ocdp"
    #define CASCADE_CONTEXT_CPU_CORES               "CASCADE/cpu_cores"
    #define CASCADE_CONTEXT_GPUS                    "CASCADE/gpus"
    #define CASCADE_CONTEXT_WORKER_CPU_AFFINITY     "CASCADE/worker_cpu_affinity"
    #define LOAD_INFO_DISSEMINATION_RATE            "INFOSST/load_info_update_rate"
    #define CACHE_INFO_DISSEMINATION_RATE           "INFOSST/cache_info_update_rate"

    /**
     * A class describing the resources available in the Cascade context.
     */
    class ResourceDescriptor {
    public:
        /** cpu cores, loaded from configuration **/
        std::vector<uint32_t> cpu_cores;
        /** worker cpu aworker cpu ffinity, loaded from configuration **/
        std::map<uint32_t,std::vector<uint32_t>> multicast_ocdp_worker_to_cpu_cores;
        std::map<uint32_t,std::vector<uint32_t>> p2p_ocdp_worker_to_cpu_cores;
        /** gpu list**/
        std::vector<uint32_t> gpus;
        /** constructor **/
        ResourceDescriptor();
        /** destructor **/
        virtual ~ResourceDescriptor();
        /** dump **/
        void dump() const;
    };

    /**
     * The cascade context
     *
     * The cascade context manages computation resources like CPU cores, GPU, and memory. It works as the container for all
     * "off-critical" path logics. The main components of cascade context includes:
     * 1 - a thread pool for the off-critical path logics.
     * 2 - a prefix registry.
     * 3 - a bounded Action buffer.
     */
    using prefix_entry_t =
                std::unordered_map<
                    std::string, // udl_id
                    std::tuple<
                        DataFlowGraph::VertexShardDispatcher,         // shard dispatcher
#ifdef HAS_STATEFUL_UDL_SUPPORT
                        DataFlowGraph::Statefulness,                  // is stateful/stateless/singlethreaded
#endif//HAS_STATEFUL_UDL_SUPPORT
                        DataFlowGraph::VertexHook,                    // hook
                        std::shared_ptr<OffCriticalDataPathObserver>, // ocdpo
                        std::vector<std::string>,                     // required object pathnames
                        std::unordered_map<std::string,bool>,         // output map{prefix->bool}
                        uint64_t                                      // expected_execution_timeus
                    >
                >;
    using match_results_t = std::unordered_map<std::string,prefix_entry_t>;
            
    template <typename... CascadeTypes>
    class CascadeContext: public ICascadeContext {
    private:
        struct action_queue {
            struct Action           action_buffer[ACTION_BUFFER_SIZE];
            std::atomic<size_t>     action_buffer_head;
            std::atomic<size_t>     action_buffer_tail;
            mutable std::mutex      action_buffer_slot_mutex;
            mutable std::mutex      action_buffer_data_mutex;
            mutable std::condition_variable action_buffer_slot_cv;
            mutable std::condition_variable action_buffer_data_cv;
            inline void initialize();
            inline void action_buffer_enqueue(Action&&);
            inline bool action_buffer_emplace(Action& action);
            inline Action action_buffer_dequeue(std::atomic<bool>& is_running);
            inline void notify_all();
        };

        /** action (ring) buffer control */
        struct action_queue unscheduled_action_queue;
#ifdef HAS_STATEFUL_UDL_SUPPORT
        std::vector<std::unique_ptr<struct action_queue>> stateful_action_queues_for_multicast;
        std::vector<std::unique_ptr<struct action_queue>> stateful_action_queues_for_p2p;
        struct action_queue single_threaded_action_queue_for_multicast;
        struct action_queue single_threaded_action_queue_for_p2p;  
#endif//HAS_STATEFUL_UDL_SUPPORT
        struct action_queue stateless_action_queue_for_multicast;
        struct action_queue stateless_action_queue_for_p2p;

        /** thread pool control */
        std::atomic<bool>       is_running;
        /** the prefix registries, one is active, the other is shadow
         * prefix->{udl_id->{ocdpo,{prefix->trigger_put/put}}
         */
        std::shared_ptr<PrefixRegistry<prefix_entry_t,PATH_SEPARATOR>> prefix_registry_ptr;
        /** the data path logic loader */
        std::unique_ptr<UserDefinedLogicManager<CascadeTypes...>> user_defined_logic_manager;
        /** the off-critical data path worker thread pools */
        std::vector<std::thread> stateless_workhorses_for_multicast;
        std::vector<std::thread> stateless_workhorses_for_p2p;
#ifdef HAS_STATEFUL_UDL_SUPPORT
        std::vector<std::thread> stateful_workhorses_for_multicast;
        std::vector<std::thread> stateful_workhorses_for_p2p;
        std::thread              single_threaded_workhorse_for_multicast;
        std::thread              single_threaded_workhorse_for_p2p;
#endif//HAS_STATEFUL_UDL_SUPPORT
        std::thread              scheduler_workhorse;  // scheduler thread
        
         /** information used by scheduler 
         * mapping between {entry_pathname of the dfg -> DataFlowGraph::TaskInfo}
         */
        std::unordered_map<std::string, DataFlowGraph::TaskInfo> prefix_to_task_info;
        // mapping betwee {model_id -> MLModelStats}, used by scheduler to decide which model to load/evict
        std::unordered_map<uint64_t, MLModelStats>      local_ml_models_stats;

        std::atomic<uint64_t>   local_queue_wait_time;
        std::set<uint32_t>      local_cached_models_info;
        std::atomic<uint64_t>      local_available_memory;
        mutable std::shared_mutex local_cached_models_info_mutex;
        std::atomic<bool>         local_cached_models_info_updated;

        std::unordered_map<node_id_t, std::set<uint32_t>>  group_cached_models_info; // include all other nodes info, beside this node
        std::unordered_map<node_id_t, uint64_t>            group_available_memory; // derived from group_cached_models_info
        std::unordered_map<node_id_t, uint64_t>            group_queue_wait_times;// include all other nodes info, beside this node
        mutable std::shared_mutex      group_cached_models_info_mutex;
        mutable std::shared_mutex      group_queue_wait_times_mutex;
        std::atomic<uint64_t>   last_group_cached_models_info_update_timeus;
        std::atomic<uint64_t>   last_group_queue_wait_times_update_timeus;

        /**
         * destroy the context, to be called in destructor
         */
        void destroy();
        /**
         * off critical data path workhorse
         * @param _1 the task id, started from 0 to (OFF_CRITICAL_DATA_PATH_THREAD_POOL_SIZE-1)
         */
        void workhorse(uint32_t,struct action_queue&);

        /**
         * scheduler workhorse: run scheduler for unscheduled arrival actions
         * @param _1 the task id, started from 0 to (OFF_CRITICAL_DATA_PATH_THREAD_POOL_SIZE-1)
         */
        void tide_scheduler_workhorse(uint32_t,struct action_queue&);

    private:
        /**
         * run scheduler on an unscheduled action, entry task
         * @param action the entry task(represented by action) to be scheduled
         * @param worker_id the worker id
        */
        void fire_scheduler(Action&& action,uint32_t worker_id);

    public:
        /** Resources **/
        const ResourceDescriptor resource_descriptor;
        /**
         * Constructor
         */
        CascadeContext();
        /**
         * construct the resources from Derecho configuration.
         *
         * We enforce an explicit call to this initialization function to avoid heavily relying on the order of C++
         * global/static variables: CascadeContext relies on the global configuration from derecho implementation, which is
         * generally initialized with commandline parameters in main(). If we initialize the CascadeContext singleton in its
         * constructor, which happens before main(), it might miss extra configuration from commandline. Therefore,
         * CascadeContext singleton needs to be initialized in main() by calling CascadeContext::construct(). Moreover, it
         * needs the off critical data path handler from main();
         *
         * @param group_ptr                         The group handle
         */
        void construct();
        /**
         * get the reference to encapsulated service client handle.
         * The reference is valid only after construct() is called.
         *
         * @return a reference to service client.
         */
        ServiceClient<CascadeTypes...>& get_service_client_ref() const;
        /**
         * We give up the following on-demand loading mechanism:
         * ==============================================================================================================
         * The prefix registry management APIs
         *
         * We separate the prefix registration in two stages: preregistration and registration to support lazy loading
         * of the data path logic packages. During preregistration stage, we create an entry for the corresponding prefix
         * in the registry with an empty value. During registration stage, the prefix is filled.
         *
         * IMPORTANT: the prefix registry management API are designed for use ONLY in the critical data path. Since the
         * critical data path is a single thread, we don't use any lock for high performance. Please keep that in mind
         * and don't touch the following APIs in prefix handlers where you have access to all the CascadeContext APIs.
         *
         * - preregister_prefixes() allows batching preregistration of a set of prefixes, previous registered OCDPO will
         * be overwritten by the new prefixes.
         * - register_prefix() setup the OCDPO for the corresponding prefix. If the ocdpo_ptr is nullptr, the prefix is
         *   "preregister"ed.
         * - unregister_prefix() deletes a corresponding prefix from registry.
         * - get_prefix_handler() returns a raw pointer to the handler.
         *
         * @param prefixes  a list of vectors to pre-register.
         * @param prefix    a prefix to register.
         * @param ocdpo_ptr the data path observer, nullptr for preregistration.
         *
         * @return get_prefix_handler returns the OffCriticalDataPathObserver it holds for the corresponding prefix. If
         * the prefix is not registered, it will return nullptr.
         *
        virtual void preregister_prefixes(const std::vector<std::string>& prefixes);
        virtual void register_prefix(const std::string& prefix, const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr = nullptr);
        virtual void unregister_prefix(const std::string& prefix);
        virtual OffCriticalDataPathObserver* get_prefix_handler(const std::string& prefix);
         * =============================================================================================================
         * Now we agree on the new design that the prefix is assumed to be registered before the critical data path saw
         * some data coming. Without a lock guarding prefix registry in the critical data path, it's a little bit tricky
         * to support runtime update.
         *
         * IMPORTANT: Successful unregistration of a prefix does not guarantee the corresponding UDL is safe to be
         * released. Because a previous triggered off-critical data path might still working on the unregistered prefix.
         * TODO: find a mechanism to trigger safe UDL unloading.
         */

        /**
         * Register a set of prefixes
         *
         * @param prefixes              - the prefixes set
         * @param user_defined_logic_hook
         *                              - the hook for this ocdpo
         * @param shard_dispatcher      - the shard dispatcher
         * @param user_defined_logic_id - the UDL id, presumably an UUID string
         * @param ocdpo_ptr             - the data path observer
         * @param required_object_pathnames - the required object pathnames, from dependency DAG joining node
         * @param outputs               - the outputs are a map from another prefix to put type (true for trigger put,
         *                                false for put).
         * @param expected_execution_timeus - the expected execution time in microseconds
         */
        virtual void register_prefixes(const std::unordered_set<std::string>& prefixes,
                                       const DataFlowGraph::VertexShardDispatcher shard_dispatcher,
#ifdef HAS_STATEFUL_UDL_SUPPORT
                                       const DataFlowGraph::Statefulness stateful,
#endif
                                       const DataFlowGraph::VertexHook hook,
                                       const std::string& user_defined_logic_id,
                                       const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr,
                                       const std::vector<std::string>& required_object_pathnames,
                                       const std::unordered_map<std::string,bool>& outputs,
                                       const uint64_t expected_execution_timeus);
        /**
         * Unregister a set of prefixes
         *
         * @param prefixes              - the prefixes set
         * @param user_defined_logic_id - the UDL id, presumably an UUID string
         * @param ocdpo_ptr             - the data path observer
         */
        virtual void unregister_prefixes(const std::unordered_set<std::string>& prefixes,
                                         const std::string& user_defined_logic_id);
        /**
         * Get the prefix handlers registered for a prefix
         *
         * @param prefix                - the prefix
         *
         * @return the unordered map of observers registered to this prefix.
         */
        virtual match_results_t get_prefix_handlers(const std::string& prefix);
        /**
         * Helper function to find the allocated worker based on the ranking of pathname
         * @param pathname        - the path name of the entry vertex of the dfg
        */
        virtual int64_t get_task_ranking(const std::string& pathname);
        /**
         * Helper function for scheduler DAG structure trigger_put, if the next task is to be executed on the same machine
         * @param key           - key name of the object
         * @param value         - the object
        */
        virtual void find_handlers_and_local_post(ObjectWithStringKey& value);
        /**
         * post an action to the Context for processing.
         *
         * @param action        The action
         * @param stateful      If the action is stateful|stateless|singlethreaded
         * @param is_trigger    True for trigger, meaning the action will be processed in the workhorses for p2p send
         *
         * @return  true for a successful post, false for failure. The current only reason for failure is to post to a
         *          context already shut down.
         */
#ifdef HAS_STATEFUL_UDL_SUPPORT
        virtual bool post(Action&& action, DataFlowGraph::Statefulness stateful, bool is_trigger);
#else
        virtual bool post(Action&& action, bool is_trigger);
#endif//HAS_STATEFUL_UDL_SUPPORT

        /**
         *  Enqueue the action to unscheduled_action_queue
         * TODO: optimize scheduler algorithm by using cached results(no on the spot scheduler computation) 
         *       and move this to server.hpp critical datapath
         * @param action        The action
         *
         * @return  true for a successful post, false for failure. The current only reason for failure is to post to a
         *          context already shut down.
         */
        bool post_to_scheduler(Action&& action);

        /**
         * Get the stateless action queue length
         *
         * @return current queue_length
         */
        virtual size_t stateless_action_queue_length_p2p();
        virtual size_t stateless_action_queue_length_multicast();

        /**
         * Helper function for scheduler:
         * check machine load on the node, based on local cache info
         * @param  node_id  the node id to query about its queueing delay
         * @return wait_time the estimated queueing wait time on that node
        */
        uint64_t check_queue_wait_time(node_id_t node_id);

        /**
         * check if certain model exist in the node's gpu, based on local cache info
         * @param  node_id  the node id to query about its models in gpu 
         * @param  model_id the model_id to check if it is in the node's gpu
         * @return bool     
        */
        bool check_if_model_in_gpu(node_id_t node_id, uint32_t model_id);

        /**
         * Helper function to get the required_models for the pathname
         * @param  pathname the model_id related to the pathname
         * @return MLModelInfo 
        */
        std::vector<DataFlowGraph::MLModelInfo> get_required_models_info(std::string pathname);

        /**
         * update local_cached_models_info, called
         * when there is local model fetching or eviction to GPU memory on this node.
        */
        void update_local_model_info(std::set<uint32_t> new_cached_models);
        
        /**
         * send local_cached_models_info to all nodes in the group
        */
       void send_local_cached_models_info();

        /** Helper function to check local cached group_cached_models_info and group_queue_wait_times
        */
        std::string local_cached_info_dump();

        /**
          * Helper function used by emit to get the scheduled node_id from adfg,
          * reschedule the task if the scheduled node is no longer a good choice.
        */
        node_id_t next_task_scheduled_node_id(bool& scheduled, 
                                              const std::string& task_name, 
                                              const std::string& adfg);

        /**
         * given a prefix of the first task of the dfg generage the adfg result for this instance
         * @param entry_prefix  entry task pathname
         * @return adfg encoded to string
        */
        std::string tide_scheduler(std::string entry_prefix);

        /**
         * given a prefix of the first task of the dfg generage the adfg result for this instance
         * @param entry_prefix  entry task pathname
         * @param entry_key     entry task key with prefix removed
         * @return adfg encoded to string
        */
        std::string hash_scheduler(std::string entry_prefix, std::string entry_key);

        /**
         * given a prefix of the first task of the dfg generage the adfg result for this instance
         * @param entry_prefix  entry task pathname
         * @param entry_key     entry task key with prefix removed
         * @return adfg encoded to string
        */
        std::string heft_scheduler(std::string entry_prefix);


        /**
         * Destructor
         */
        virtual ~CascadeContext();
    };//CascadeContext
} // cascade
} // derecho

#include "detail/service_impl.hpp"
