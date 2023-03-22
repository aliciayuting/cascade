#pragma once

class DefaultOffCriticalDataPathObserver : public IDefaultOffCriticalDataPathObserver, public OffCriticalDataPathObserver {
public:
    virtual void operator() (
            const node_id_t sender,
            const std::string& full_key_string,
            const uint32_t prefix_length,
            persistent::version_t version,
            const mutils::ByteRepresentable* const value_ptr,
            const std::unordered_map<std::string,bool>& outputs,
            ICascadeContext* ctxt,
            uint32_t worker_id,
            std::string adfg) override;

   /**
    * This function is a lot inefficient comparing to the above function, since it takes a copy of vector of objects
    * Currently this function is only used during the joint point of the DAG, where the value sizes are small 
   */
    virtual void operator() (
            const node_id_t sender,
            const std::string& full_key_string,
            const uint32_t prefix_length,
            persistent::version_t version,
            const std::vector<std::shared_ptr<mutils::ByteRepresentable>>& value_ptrs,
            const std::unordered_map<std::string,bool>& outputs,
            ICascadeContext* ctxt,
            uint32_t worker_id,
            std::string adfg){}

    virtual void ocdpo_handler (
            const node_id_t                 sender,
            const std::string&              object_pool_pathname,
            const std::string&              key_string,
            const ObjectWithStringKey&      object,
            const emit_func_t&              emit,
            DefaultCascadeContextType*      typed_ctxt,
            uint32_t                        worker_id) = 0;

    virtual void ocdpo_handler (
            const node_id_t                 sender,
            const std::string&              object_pool_pathname,
            const std::string&              key_string,
            std::vector<ObjectWithStringKey>      objects,
            const emit_func_t&              emit,
            DefaultCascadeContextType*      typed_ctxt,
            uint32_t                        worker_id){}
};
