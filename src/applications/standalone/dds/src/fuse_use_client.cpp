#include <cascade_dds/dds.hpp>
#include <iomanip>
#include <iostream>
#include <tuple>
#include <vector>
#include <functional>
#include <mutex>
#include <string>
#include <fstream>
#include <readline/readline.h>
#include <readline/history.h>
#include <sys/prctl.h>
#include <unistd.h>
#include <cascade/object.hpp>
#include <derecho/conf/conf.hpp>

using namespace derecho::cascade;

struct message_header_t {
    uint32_t seqno;
    mutable uint64_t sending_ts_us;
};

#define check_put_and_remove_result(result) \
    for (auto& reply_future:result.get()) {\
        auto reply = reply_future.second.get();\
        std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)\
                  << ",ts_us:" << std::get<1>(reply) << std::endl;\
    }




static void setup(DDSClient& client) {
    auto& capi = client.capi;
    std::string objp("/dds/metadata");
    std::string affinity_set_regex;
    auto result = capi.create_object_pool<PersistentCascadeStoreWithStringKey>(
            objp,
            0,
            sharding_policy_type::HASH,
            {},
            affinity_set_regex);
    check_put_and_remove_result(result);
    std::string objp2("/dds/tiny_text");
    auto result2 = capi.create_object_pool<VolatileCascadeStoreWithStringKey>(
            objp2,
            0,
            sharding_policy_type::HASH,
            {},
            affinity_set_regex);
    check_put_and_remove_result(result2);
    usleep(1000);
}

static void test_pub_sub(
        DDSMetadataClient& metadata_client,
        DDSClient& client) {
    
    // Create topic
    std::string topic_name("test_topic");
    std::string objp("/dds/tiny_text");
    Topic topic(topic_name,objp);
    metadata_client.create_topic(topic);

    std::cout << "Finished create topic" << std::endl;

    // subscribe
    auto subscriber = client.template subscribe<std::string>(
            topic_name,
            std::unordered_map<std::string,message_handler_t<std::string>>{{
                std::string{"default"},
                [topic_name](const std::string& msg){
                    std::cout << "Message of " << msg.size() << " bytes"
                                << " received in topic '" << topic_name << "': " << msg << std::endl;
                }
            }}
    );
    
    std::cout << "Finished subscribe" << std::endl;

    // publish
    uint32_t num_messages = 2;
    auto publisher = client.template create_publisher<std::string>(topic_name);
    if (!publisher) {
        std::cerr << "failed to create publisher for topic:" << topic_name << std::endl;
        return;
    }
    std::cout << "publisher created for topic:" << publisher->get_topic() << std::endl;;
    for (uint32_t i=0;i<num_messages;i++) {
        const std::string msg = std::string("Message #" + std::to_string(i) + " in topic " + publisher->get_topic());
        publisher->send(msg);
    }
    std::cout << "Finished publish" << std::endl;
    usleep(10000);
}

int main(int argc, char** argv) {
    std::cout << "Cascade DDS Client" << std::endl;
    auto dds_config = DDSConfig::get();
    auto metadata_client = DDSMetadataClient::create(dds_config);
    auto client = DDSClient::create(dds_config);
    setup(*client);
    test_pub_sub(*metadata_client,*client);
    
    return 0;
}
