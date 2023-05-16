#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>

namespace derecho{
namespace cascade{

#define MY_UUID     "49a12a9b-7400-11ab-8490-0242ac110002"
#define MY_DESC     "Demo DLL UDL 2 that printing what ever received on console."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class ConsolePrinterTwoOCDPO: public DefaultOffCriticalDataPathObserver {
    virtual void ocdpo_handler (
            const node_id_t             sender,
            const std::string&          object_pool_pathname,
            const std::string&          key_string,
            const ObjectWithStringKey&  object,
            const emit_func_t&          emit,
            DefaultCascadeContextType*  typed_ctxt,
            uint32_t                    worker_id) override {
        dbg_default_trace("------- 2. in ConsolePrinterOneOCDPO::OffCriticalDataPathObserver  ------");
        emit(key_string,EMIT_NO_VERSION_AND_TIMESTAMP,object.blob);
    }

    virtual void ocdpo_handler (
            const node_id_t                 sender,
            const std::string&              object_pool_pathname,
            const std::string&              key_string,
            std::vector<ObjectWithStringKey>      object,
            const emit_func_t&              emit,
            DefaultCascadeContextType*      typed_ctxt,
            uint32_t                        worker_id) override {
        // std::cout << "[csharp ocdpo]: calling into managed code from sender=" << sender << 
        //     " with key=" << key_string << std::endl;
        dbg_default_trace("NOO should not get here! ------- 2. in ConsolePrinterTwoOCDPO::OffCriticalDataPathObserver professing multiple objects  ------");
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<ConsolePrinterTwoOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> ConsolePrinterTwoOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    ConsolePrinterTwoOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return ConsolePrinterTwoOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
