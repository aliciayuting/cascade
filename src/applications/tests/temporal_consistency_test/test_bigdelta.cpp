#include <cascade/service_client_api.hpp>
#include <cascade/object.hpp>
#include <cascade/utils.hpp>
#include <cascade/service_types.hpp>
#include <derecho/utils/time.h>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <vector>
#include <cstring>
#include <cstdlib>

using namespace derecho::cascade;

// 30KB chunk size
constexpr size_t CHUNK_SIZE = 30 * 1024;

/**
 * Test client for big delta puts
 * 
 * This program puts 30KB chunks of data at a configurable rate.
 * 
 * Usage: test_bigdelta <puts_per_second> <duration_seconds>
 * 
 * Arguments:
 *   puts_per_second  - How many puts per second (can be float or int, e.g., 10 or 0.5)
 *   duration_seconds - How long to run the test in seconds (can be float or int)
 */
int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <puts_per_second> <duration_seconds>" << std::endl;
        std::cerr << "  puts_per_second  - How many puts per second (can be float, e.g., 10 or 0.5)" << std::endl;
        std::cerr << "  duration_seconds - How long to run in seconds (can be float, e.g., 5 or 2.5)" << std::endl;
        return 1;
    }

    // Parse arguments as doubles to support both int and float inputs
    double puts_per_second = std::stod(argv[1]);
    double duration_seconds = std::stod(argv[2]);

    if (puts_per_second <= 0) {
        std::cerr << "Error: puts_per_second must be positive" << std::endl;
        return 1;
    }
    if (duration_seconds <= 0) {
        std::cerr << "Error: duration_seconds must be positive" << std::endl;
        return 1;
    }

    // Calculate interval between puts in microseconds
    double interval_us = 1000000.0 / puts_per_second;
    // Total duration in microseconds
    double total_duration_us = duration_seconds * 1000000.0;

    std::cout << "=== Big Delta Put Test ===" << std::endl;
    std::cout << "Chunk size: " << CHUNK_SIZE << " bytes (30KB)" << std::endl;
    std::cout << "Puts per second: " << puts_per_second << std::endl;
    std::cout << "Duration: " << duration_seconds << " seconds" << std::endl;
    std::cout << "Interval between puts: " << interval_us << " microseconds" << std::endl;
    std::cout << std::endl;

    try {
        // Initialize the Cascade client (singleton)
        auto& capi = ServiceClientAPI::get_service_client();

        // Use subgroup index 0, shard index 0 for testing
        uint32_t subgroup_index = 0;
        uint32_t shard_index = 0;

        // Create a 30KB data buffer filled with a pattern
        std::vector<uint8_t> data_buffer(CHUNK_SIZE);
        for (size_t i = 0; i < CHUNK_SIZE; i++) {
            data_buffer[i] = static_cast<uint8_t>(i % 256);
        }

        // Track statistics
        uint64_t put_count = 0;
        uint64_t success_count = 0;
        uint64_t failure_count = 0;

        // Get start time
        auto start_time = std::chrono::high_resolution_clock::now();
        auto next_put_time = start_time;

        std::cout << "Starting puts..." << std::endl;

        while (true) {
            // Check if we've exceeded the duration
            auto current_time = std::chrono::high_resolution_clock::now();
            auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                current_time - start_time).count();
            
            if (elapsed_us >= total_duration_us) {
                break;
            }

            // Wait until the next scheduled put time
            std::this_thread::sleep_until(next_put_time);

            // Create the object with a unique key for each put
            ObjectWithStringKey obj;
            obj.key = "bigdelta_key_" + std::to_string(put_count);
            obj.blob = Blob(data_buffer.data(), CHUNK_SIZE);
            obj.previous_version = persistent::INVALID_VERSION;
            obj.previous_version_by_key = persistent::INVALID_VERSION;
            obj.set_message_id(put_count);

            // Get current time in microseconds for put_by_time
            uint64_t current_time_us = get_walltime() / 1000ULL;

            try {
                auto result = capi.template put_by_time<PersistentCascadeStoreWithStringKey>(
                    obj, current_time_us, subgroup_index, shard_index, false);

                // Wait for result
                bool got_result = false;
                for (auto& reply_future : result.get()) {
                    auto reply = reply_future.second.get();
                    got_result = true;
                    if (put_count % 100 == 0) {
                        std::cout << "  Put " << put_count << ": Version " << std::get<0>(reply)
                                  << ", Timestamp " << std::get<1>(reply) << " us" << std::endl;
                    }
                }
                if (got_result) {
                    success_count++;
                } else {
                    failure_count++;
                    std::cerr << "  Put " << put_count << ": Empty result (rejected)" << std::endl;
                }
            } catch (const derecho::derecho_exception& e) {
                failure_count++;
                std::cerr << "  Put " << put_count << ": Exception - " << e.what() << std::endl;
            } catch (const std::exception& e) {
                failure_count++;
                std::cerr << "  Put " << put_count << ": Exception - " << e.what() << std::endl;
            }

            put_count++;
            
            // Schedule next put
            next_put_time += std::chrono::microseconds(static_cast<int64_t>(interval_us));
        }

        // Final statistics
        auto end_time = std::chrono::high_resolution_clock::now();
        auto actual_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();

        std::cout << std::endl;
        std::cout << "=== Test Complete ===" << std::endl;
        std::cout << "Total puts attempted: " << put_count << std::endl;
        std::cout << "Successful puts: " << success_count << std::endl;
        std::cout << "Failed puts: " << failure_count << std::endl;
        std::cout << "Actual duration: " << actual_duration_ms << " ms" << std::endl;
        
        if (actual_duration_ms > 0) {
            double actual_rate = static_cast<double>(put_count) * 1000.0 / actual_duration_ms;
            std::cout << "Actual put rate: " << std::fixed << std::setprecision(2) 
                      << actual_rate << " puts/sec" << std::endl;
            
            double throughput_mbps = (static_cast<double>(success_count) * CHUNK_SIZE * 8.0) 
                                     / (actual_duration_ms / 1000.0) / 1000000.0;
            std::cout << "Throughput: " << throughput_mbps << " Mbps" << std::endl;
        }

        // Flush logs on all shards in subgroup 0 of PersistentCascadeStoreWithStringKey
        std::cout << std::endl;
        std::cout << "=== Flushing logs on all shards ===" << std::endl;
        {
            auto shards = capi.get_subgroup_members<VolatileCascadeStoreWithStringKey>(subgroup_index);
            std::cout << "  Sending flush_log to " << shards.size() << " shards in subgroup " << subgroup_index << " ..." << std::endl;
            
            ObjectWithStringKey flush_obj;
            std::string flush_value = "flush";
            flush_obj.blob = Blob(reinterpret_cast<const uint8_t*>(flush_value.c_str()), flush_value.size());
            flush_obj.previous_version = persistent::INVALID_VERSION;
            flush_obj.previous_version_by_key = persistent::INVALID_VERSION;
            
            uint32_t shard_id = 0;
            for (auto& shard : shards) {
                flush_obj.key = "/flush_log/" + std::to_string(shard_id);
                std::cout << "    Sending flush to shard " << shard_id << " with key: " << flush_obj.key << std::endl;
                
                // Send put to each node in the shard to ensure all replicas receive it
                for (size_t j = 0; j < shard.size(); j++) {
                    // Each iteration reaches a different node in the shard due to round robin policy
                    auto res = capi.template put<PersistentCascadeStoreWithStringKey>(flush_obj, subgroup_index, shard_id, true);
                    for (auto& reply_future : res.get()) {
                        reply_future.second.get(); // Wait for the put to complete
                    }
                }
                std::cout << "    ✓ Flush sent to shard " << shard_id << std::endl;
                shard_id++;
            }
            std::cout << "  ✓ All shards flushed" << std::endl;
        }
        std::cout << std::endl;

        // Flush client log
        TimestampLogger::flush("client.dat");
        std::cout << "[flush log]: I have flushed the log name client.dat" << std::endl;

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return -1;
    }
}
