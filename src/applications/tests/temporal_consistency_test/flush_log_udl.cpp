#include <cascade/user_defined_logic_interface.hpp>
#include <cascade/service_client_api.hpp>
#include <iostream>

namespace derecho{
namespace cascade{

#define MY_UUID     "48e60f7c-8500-11eb-8755-0242ac110002"
#define MY_DESC     "UDL that flush the log for perf measurement."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class FlushLogOCDPO: public OffCriticalDataPathObserver {
    virtual void operator () (const node_id_t,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
        TimestampLogger::flush("node" + std::to_string(typed_ctxt->get_service_client_ref().get_my_id()) + ".dat");
        std::cout << "[flush log ocdpo]: I have flushed the log name" << "node" + std::to_string(typed_ctxt->get_service_client_ref().get_my_id()) + ".dat";
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<FlushLogOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> FlushLogOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    FlushLogOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return FlushLogOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
