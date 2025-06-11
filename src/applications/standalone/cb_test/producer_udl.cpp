#pragma once

#include <iostream>
#include <string>

#include <cascade/user_defined_logic_interface.hpp>
#include <cascade/utils.hpp>
#include <cascade/cascade_interface.hpp>

namespace derecho {
namespace cascade {

#define MY_UUID "11a3c123-3300-31ac-1866-0003ac330000"
#define MY_DESC "UDL to send oob."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class ProducerOCDPO : public DefaultOffCriticalDataPathObserver {

private:
    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
    uint64_t head_ptr;
    uint64_t tail_ptr;
    uint64_t rkey;
    uint64_t cbuffer_ptr;
    void* oob_mr_ptr = nullptr;


    
    // Hardcode to run on n0
    virtual void ocdpo_handler(const node_id_t sender,
                               const std::string& object_pool_pathname,
                               const std::string& key_string,
                               const ObjectWithStringKey& object,
                               const emit_func_t& emit,
                               DefaultCascadeContextType* typed_ctxt,
                               uint32_t worker_id) {
        std::cout << "[OOB]: I Producer (" << worker_id << ") received an object" << sender
                    << " with key=" << key_string << std::endl;
        if (key_string.find("init_msg") != std::string::npos) {
            std::cout << "\"init_msg\" found in the string.\n";
            std::string message(reinterpret_cast<const char*>(object.blob.bytes), object.blob.size);
            // message is in the format "head_ptr:tail_ptr:rkey:cbuffer_ptr"
            // auto tokens = str_tokenizer(message, ':');
            auto tokens = str_tokenizer(message);
            if (tokens.size() != 4) {
                std::cerr << "Invalid init_msg format: " << message  << "message size: " <<  message.size() <<"token size: "<<tokens.size() << std::endl;
                return;
            }
            try {
                head_ptr = std::stoull(tokens[0]);
                std::cout << "Parsed head_ptr: " << head_ptr << std::endl;
                tail_ptr = std::stoull(tokens[1]);
                std::cout << "Parsed tail_ptr: " << tail_ptr << std::endl;
                rkey = std::stoull(tokens[2]);
                std::cout << "Parsed rkey: " << rkey << std::endl;
                cbuffer_ptr = std::stoull(tokens[3]);
            } catch (const std::exception& e) {
                std::cerr << "Error parsing init_msg: " << e.what() << std::endl;
                return;
            }
            std::cout << "Parsed init_msg: head_ptr=" << head_ptr
                      << ", tail_ptr=" << tail_ptr
                      << ", rkey=" << rkey
                      << ", cbuffer_ptr=" << cbuffer_ptr << std::endl;
            return;
        } 
        // produce oob data and send to ConsumerOOB
    }

public:
   

    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<ProducerOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
   
};

std::shared_ptr<OffCriticalDataPathObserver> ProducerOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    ProducerOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(ICascadeContext* ctxt, 
                                                        const nlohmann::json& config) {
    return ProducerOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

}  // namespace cascade
}  // namespace derecho