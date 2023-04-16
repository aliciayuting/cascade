#include <cascade/detail/_user_defined_logic_interface.hpp>
#include <regex>

namespace derecho {
namespace cascade {



void DefaultOffCriticalDataPathObserver::operator() (
        const node_id_t sender,
        const std::string& full_key_string,
        const uint32_t prefix_length,
        persistent::version_t,
        const mutils::ByteRepresentable* const value_ptr,
        const std::unordered_map<std::string,bool>& outputs,
        ICascadeContext* ctxt,
        uint32_t worker_id,
        std::string adfg) {
    auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
    const auto* object_ptr = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
    std::string object_pool_pathname = full_key_string.substr(0,prefix_length);
    while (object_pool_pathname.back() == PATH_SEPARATOR && !object_pool_pathname.empty()) {
        object_pool_pathname.pop_back();
    }
    std::string key_string = full_key_string.substr(prefix_length);

    // call typed handler
    dbg_default_trace("DefaultOffCriticalDataPathObserver: calling typed handler for key={}...", full_key_string);
    this->ocdpo_handler(
            sender,
            object_pool_pathname,
            key_string,
            *object_ptr,
            [&](const std::string&    key,
                persistent::version_t version,
                uint64_t              timestamp_us,
                persistent::version_t previous_version,
                persistent::version_t previous_version_by_key,
#ifdef ENABLE_EVALUATION
                uint64_t              message_id,
#endif
                const Blob& blob) {
                    std::string pre_adfg_pathname = object_pool_pathname;
                    if(object_pool_pathname.back() != PATH_SEPARATOR) {
                        pre_adfg_pathname = object_pool_pathname + PATH_SEPARATOR;
                    }
                    pre_adfg_t pre_adfg = typed_ctxt->get_pre_adfg(pre_adfg_pathname);
                    std::regex rgx(",");
                    std::sregex_token_iterator end;
                    std::vector<std::string> sorted_pathnames;
                    if(!pre_adfg.empty()){
                        sorted_pathnames = std::get<3>(pre_adfg.at(pre_adfg_pathname));
                    }
                
                /** TODO: check adfg to find out the machines!! */
                for (const auto& okv: outputs) {
                    std::string prefix = okv.first;
                    while (!prefix.empty() && prefix.back() == PATH_SEPARATOR) prefix.pop_back();
                    std::string new_key = (prefix.empty()? key : prefix+PATH_SEPARATOR+key);
                    dbg_default_trace("~~~~~Object to send: key[{}] ~~~~~", new_key);
                    // emplace constructor to avoid copy:
                    ObjectWithStringKey obj_to_send(
#ifdef ENABLE_EVALUATION
                            message_id,
#endif
                            version,
                            timestamp_us,
                            previous_version,
                            previous_version_by_key,
                            new_key,
                            adfg,
                            key,
                            blob,
                            true);
                    bool scheduled = false;
                    node_id_t scheduled_node_id;
                    if((!prefix.empty()) && (!sorted_pathnames.empty())){
                        auto itr = std::find(sorted_pathnames.begin(), sorted_pathnames.end(), prefix + PATH_SEPARATOR);
                        if(itr != sorted_pathnames.end() ){
                            auto position = std::distance(sorted_pathnames.begin(), itr);
                            std::sregex_token_iterator iter(adfg.begin(), adfg.end(), rgx, -1);
                            for(; iter != end && position > 0; iter++){
                                position --;
                            }
                            scheduled = true;
                            scheduled_node_id = static_cast<uint32_t>(std::stoul(*iter));
                        }
                    }
                    if(scheduled){
                        if(scheduled_node_id != typed_ctxt->get_service_client_ref().get_my_id()){
                            dbg_default_trace("~~~~~~ scheduled the next node to different node in emit ~~~~~~~");
                            typed_ctxt->get_service_client_ref().single_node_trigger_put(obj_to_send, scheduled_node_id);
                        }else{
                            dbg_default_trace("~~~~~~ scheduled the next node to the same node in emit ~~~~~~~");
                            typed_ctxt->find_handlers_and_local_post(obj_to_send);
                        }
                        
                        
                    }else{
                        if (okv.second) {
                            // temp fix since prefix does not have corresponding object pool in console_printer example
                            typed_ctxt->get_service_client_ref().put<VolatileCascadeStoreWithStringKey>(obj_to_send, 0, 0);
                        } else {
                            typed_ctxt->get_service_client_ref().put_and_forget(obj_to_send);
                        }
                    }
                }
            },
            typed_ctxt,
            worker_id);
    dbg_default_trace("DefaultOffCriticalDataPathObserver: calling typed handler for key={}...done", full_key_string);
}


void DefaultOffCriticalDataPathObserver::operator() (
        const node_id_t sender,
        const std::string& full_key_string,
        const uint32_t prefix_length,
        persistent::version_t,
        const std::vector<std::shared_ptr<mutils::ByteRepresentable>>& value_ptrs,
        const std::unordered_map<std::string,bool>& outputs,
        ICascadeContext* ctxt,
        uint32_t worker_id,
        std::string adfg) {
    auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
    std::vector<ObjectWithStringKey>  objects;
    for (const auto& value_ptr: value_ptrs) {
        const auto* object_ptr = dynamic_cast<const ObjectWithStringKey*>(value_ptr.get());
        objects.emplace_back(*object_ptr);
    }

    std::string object_pool_pathname = full_key_string.substr(0,prefix_length);
    while (object_pool_pathname.back() == PATH_SEPARATOR && !object_pool_pathname.empty()) {
        object_pool_pathname.pop_back();
    }
    std::string key_string = full_key_string.substr(prefix_length);

    // call typed handler
    dbg_default_trace("DefaultOffCriticalDataPathObserver: calling typed handler for key={}...", full_key_string);
    
    /** TODO: Clean up emit code
     *  emit function is duplicated from above. Write the emit function separately and reuse it*/
    this->ocdpo_handler(
            sender,
            object_pool_pathname,
            key_string,
            objects,
            [&](const std::string&    key,
                persistent::version_t version,
                uint64_t              timestamp_us,
                persistent::version_t previous_version,
                persistent::version_t previous_version_by_key,
#ifdef ENABLE_EVALUATION
                uint64_t              message_id,
#endif
                const Blob& blob) {
                    std::string pre_adfg_pathname = object_pool_pathname;
                    if(object_pool_pathname.back() != PATH_SEPARATOR) {
                        pre_adfg_pathname = object_pool_pathname + PATH_SEPARATOR;
                    }
                    pre_adfg_t pre_adfg = typed_ctxt->get_pre_adfg(pre_adfg_pathname);
                    std::regex rgx(",");
                    std::sregex_token_iterator end;
                    std::vector<std::string> sorted_pathnames;
                    if(!pre_adfg.empty()){
                        sorted_pathnames = std::get<3>(pre_adfg.at(pre_adfg_pathname));
                    }
                
                /** TODO: check adfg to find out the machines!! */
                for (const auto& okv: outputs) {
                    std::string prefix = okv.first;
                    while (!prefix.empty() && prefix.back() == PATH_SEPARATOR) prefix.pop_back();
                    std::string new_key = (prefix.empty()? key : prefix+PATH_SEPARATOR+key);
                    dbg_default_trace("~~~~~Object to send: key[{}] ~~~~~", new_key);
                    // emplace constructor to avoid copy:
                    ObjectWithStringKey obj_to_send(
#ifdef ENABLE_EVALUATION
                            message_id,
#endif
                            version,
                            timestamp_us,
                            previous_version,
                            previous_version_by_key,
                            new_key,
                            adfg,
                            key,
                            blob,
                            true);
                    bool scheduled = false;
                    node_id_t scheduled_node_id;
                    if((!prefix.empty()) && (!sorted_pathnames.empty())){
                        auto itr = std::find(sorted_pathnames.begin(), sorted_pathnames.end(), prefix + PATH_SEPARATOR);
                        if(itr != sorted_pathnames.end() ){
                            auto position = std::distance(sorted_pathnames.begin(), itr);
                            std::sregex_token_iterator iter(adfg.begin(), adfg.end(), rgx, -1);
                            for(; iter != end && position > 0; iter++){
                                position --;
                            }
                            scheduled = true;
                            scheduled_node_id = static_cast<uint32_t>(std::stoul(*iter));
                        }
                    }
                    if(scheduled){
                        if(scheduled_node_id != typed_ctxt->get_service_client_ref().get_my_id()){
                            dbg_default_trace("~~~~~~ scheduled the next node to different node in emit ~~~~~~~");
                            typed_ctxt->get_service_client_ref().single_node_trigger_put(obj_to_send, scheduled_node_id);
                        }else{
                            dbg_default_trace("~~~~~~ scheduled the next node to the same node in emit ~~~~~~~");
                            typed_ctxt->find_handlers_and_local_post(obj_to_send);
                        }
                        
                        
                    }else{
                        if (okv.second) {
                            // temp fix since prefix does not have corresponding object pool in console_printer example
                            typed_ctxt->get_service_client_ref().put<VolatileCascadeStoreWithStringKey>(obj_to_send, 0, 0);
                        } else {
                            typed_ctxt->get_service_client_ref().put_and_forget(obj_to_send);
                        }
                    }
                }
            },
            typed_ctxt,
            worker_id);
    dbg_default_trace("DefaultOffCriticalDataPathObserver: calling typed handler for key={}...done", full_key_string);
}

}
}
