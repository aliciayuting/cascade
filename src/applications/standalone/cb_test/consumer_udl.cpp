#pragma once

#include <iostream>


#include <cascade/user_defined_logic_interface.hpp>
#include <cascade/utils.hpp>
#include <cascade/cascade_interface.hpp>

namespace derecho {
namespace cascade {

#define MY_UUID "11a1c000-1100-11ac-1100-0001ac110001"
#define MY_DESC "UDL to receive oob."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class ConsumerOCDPO : public DefaultOffCriticalDataPathObserver {

private:
    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
    uint64_t head_ptr;
    uint64_t tail_ptr;
    uint64_t rkey;
    uint64_t cbuffer_ptr;
    void* oob_mr_ptr = nullptr;

    // Hardcode to run on n1
    virtual void ocdpo_handler(const node_id_t sender,
                               const std::string& object_pool_pathname,
                               const std::string& key_string,
                               const ObjectWithStringKey& object,
                               const emit_func_t& emit,
                               DefaultCascadeContextType* typed_ctxt,
                               uint32_t worker_id) {
        std::cout << "[OOB]: I Consumer (" << worker_id << ") received an object" << sender
                    << " with key=" << key_string << std::endl;
        if (key_string.find("init_cb") != std::string::npos) {
            // TODO: properly initialize the pointers and circular buffer
            // 1. Initialize circular buffer 
            head_ptr = 1;
            tail_ptr = 0;
            rkey = 0;
            cbuffer_ptr = 0;
            std::cout << "\"init_cb\" found in the string.\n";
            // 2. Send the circular buffer information to the producer
            std::string message = std::to_string(head_ptr) + "/" + std::to_string(tail_ptr) + "/" +
                                  std::to_string(rkey) + "/" + std::to_string(cbuffer_ptr);
            ObjectWithStringKey init_msg("/cb_send/init_msg", Blob(reinterpret_cast<const uint8_t*>(message.c_str()), message.size()));
            typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(init_msg, 0, 0);
            std::cout << "Sent init_msg with head_ptr=" << head_ptr
                      << ", tail_ptr=" << tail_ptr
                      << ", rkey=" << rkey
                      << ", cbuffer_ptr=" << cbuffer_ptr << std::endl;
        }

        return;
    }

public:
   

    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<ConsumerOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
   
};

std::shared_ptr<OffCriticalDataPathObserver> ConsumerOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    ConsumerOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(ICascadeContext* ctxt, 
                                                        const nlohmann::json& config) {
    return ConsumerOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

}  // namespace cascade
}  // namespace derecho