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
#include <string>
#include <cstring>
#include <cstdlib>
#include <algorithm>

using namespace derecho::cascade;

// 30KB chunk size
constexpr size_t CHUNK_SIZE = 30 * 1024;

/**
 * Test client for big delta puts
 * 
 * This program puts 30KB chunks of data at a configurable rate.
 * 
 * Usage: test_bigdelta <puts_per_second> <duration_seconds> <put_type> <mode>
 * 
 * Arguments:
 *   puts_per_second  - How many puts per second (can be float or int, e.g., 10 or 0.5)
 *   duration_seconds - How long to run the test in seconds (can be float or int)
 *   put_type         - "put_with_time" or "put" (determines put and get methods)
 *   mode             - "normal" (put+get), "get" (get only), or "put_pause" (pause 30s halfway through puts)
 */
int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <puts_per_second> <duration_seconds> <put_type> <mode>" << std::endl;
        std::cerr << "  puts_per_second  - How many puts per second (can be float, e.g., 10 or 0.5)" << std::endl;
        std::cerr << "  duration_seconds - How long to run in seconds (can be float, e.g., 5 or 2.5)" << std::endl;
        std::cerr << "  put_type         - \"put_with_time\" or \"put\"" << std::endl;
        std::cerr << "  mode             - \"normal\" (put+get), \"get\" (get only), or \"put_pause\" (pause 30s halfway)" << std::endl;
        return 1;
    }

    // Parse arguments as doubles to support both int and float inputs
    double puts_per_second = std::stod(argv[1]);
    double duration_seconds = std::stod(argv[2]);
    
    // Parse put_type argument
    std::string put_type = argv[3];
    bool use_put_by_time = (put_type == "put_with_time");
    if (put_type != "put_with_time" && put_type != "put") {
        std::cerr << "Error: put_type must be \"put_with_time\" or \"put\"" << std::endl;
        return 1;
    }
    
    // Parse mode argument
    std::string mode = argv[4];
    if (mode != "normal" && mode != "get" && mode != "put_pause") {
        std::cerr << "Error: mode must be \"normal\", \"get\", or \"put_pause\"" << std::endl;
        return 1;
    }

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
    std::cout << "Put type: " << put_type << std::endl;
    std::cout << "Mode: " << mode << std::endl;
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

        // Calculate expected number of puts for put_pause mode
        uint64_t expected_puts = static_cast<uint64_t>(puts_per_second * duration_seconds);
        uint64_t halfway_point = expected_puts / 2;

        // Only run puts if mode is "normal" or "put_pause"
        if (mode == "normal" || mode == "put_pause") {
            // Get start time
            auto start_time = std::chrono::high_resolution_clock::now();
            auto next_put_time = start_time;

            std::cout << "Starting puts..." << std::endl;
            if (mode == "put_pause") {
                std::cout << "Will pause for 30 seconds after " << halfway_point << " puts" << std::endl;
            }

            while (true) {
                // Check if we've exceeded the duration
                auto current_time = std::chrono::high_resolution_clock::now();
                auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    current_time - start_time).count();
                
                if (elapsed_us >= total_duration_us) {
                    break;
                }

                // In put_pause mode, pause for 30 seconds at halfway point
                if (mode == "put_pause" && put_count == halfway_point) {
                    std::cout << std::endl;
                    std::cout << "=== Pausing for 30 seconds at halfway point (after " << put_count << " puts) ===" << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(30));
                    std::cout << "=== Resuming puts ===" << std::endl;
                    // Reset the next_put_time to now to avoid catching up
                    next_put_time = std::chrono::high_resolution_clock::now();
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

                try {
                    bool got_result = false;

                    if (use_put_by_time) {
                        // Get current time in microseconds for put_by_time
                        TimestampLogger::log(1006, capi.get_my_id(), put_count);
                        uint64_t current_time_us = get_walltime() / 1000ULL;

                        auto result = capi.template put_by_time<PersistentCascadeStoreWithStringKey>(
                            obj, current_time_us, subgroup_index, shard_index, false);

                        // Wait for result
                        for (auto& reply_future : result.get()) {
                            auto reply = reply_future.second.get();
                            got_result = true;
                            if (put_count % 100 == 0) {
                                std::cout << "  Put " << put_count << ": Version " << std::get<0>(reply)
                                          << ", Timestamp " << std::get<1>(reply) << " us" << std::endl;
                            }
                        }
                    } else {
                        // Normal put
                        auto result = capi.template put<PersistentCascadeStoreWithStringKey>(
                            obj, subgroup_index, shard_index, false);

                        // Wait for result
                        for (auto& reply_future : result.get()) {
                            auto reply = reply_future.second.get();
                            got_result = true;
                            if (put_count % 100 == 0) {
                                std::cout << "  Put " << put_count << ": Version " << std::get<0>(reply)
                                          << ", Timestamp " << std::get<1>(reply) << " us" << std::endl;
                            }
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
            std::cout << "=== Put Test Complete ===" << std::endl;
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
        } else {
            // In "get" mode, calculate expected put_count for get operations
            put_count = expected_puts;
            std::cout << "Skipping puts (get mode). Will test get for " << put_count << " keys." << std::endl;
        }

        // ========== GET LATENCY TEST ==========
        // Only run get test for "normal" and "get" modes
        if (mode == "normal" || mode == "get") {
            std::cout << std::endl;
            std::cout << "=== Get Latency Test ===" << std::endl;
            std::cout << "Sleeping for 5 seconds before starting get test..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));

            // Test get latency for all keys that were put
            std::vector<double> get_latencies_us;
            uint64_t get_success_count = 0;
            uint64_t get_failure_count = 0;

            std::cout << "Testing " << (use_put_by_time ? "get_by_time" : "regular get") 
                      << " latency for " << put_count << " keys..." << std::endl;

            for (uint64_t i = 0; i < put_count; i++) {
                std::string key = "bigdelta_key_" + std::to_string(i);
                
                try {
                    // Record start time
                    auto get_start = std::chrono::high_resolution_clock::now();

                    if (use_put_by_time) {
                        // Use get_by_time with current time minus a small delta to ensure data is stable
                        uint64_t query_time_us = get_walltime() / 1000ULL - 1000000ULL; // 1 second ago
                        
                        auto result = capi.template get_by_time<PersistentCascadeStoreWithStringKey>(
                            key, query_time_us, true, subgroup_index, shard_index);
                        
                        // Wait for result
                        for (auto& reply_future : result.get()) {
                            auto obj = reply_future.second.get();
                            if (!obj.is_null()) {
                                get_success_count++;
                            } else {
                                get_failure_count++;
                            }
                        }
                    } else {
                        // Use regular get with CURRENT_VERSION
                        auto result = capi.template get<PersistentCascadeStoreWithStringKey>(
                            key, CURRENT_VERSION, true, subgroup_index, shard_index);
                        
                        // Wait for result
                        for (auto& reply_future : result.get()) {
                            auto obj = reply_future.second.get();
                            if (!obj.is_null()) {
                                get_success_count++;
                            } else {
                                get_failure_count++;
                            }
                        }
                    }

                    // Record end time and calculate latency
                    auto get_end = std::chrono::high_resolution_clock::now();
                    double latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        get_end - get_start).count();
                    get_latencies_us.push_back(latency_us);

                    if (i % 100 == 0) {
                        std::cout << "  Get " << i << ": latency " << latency_us << " us" << std::endl;
                    }
                } catch (const std::exception& e) {
                    get_failure_count++;
                    std::cerr << "  Get " << i << ": Exception - " << e.what() << std::endl;
                }
            }

            // Calculate and print get latency statistics
            std::cout << std::endl;
            std::cout << "=== Get Latency Results ===" << std::endl;
            std::cout << "Get mode: " << (use_put_by_time ? "get_by_time" : "regular get") << std::endl;
            std::cout << "Total gets: " << put_count << std::endl;
            std::cout << "Successful gets: " << get_success_count << std::endl;
            std::cout << "Failed gets: " << get_failure_count << std::endl;

            if (!get_latencies_us.empty()) {
                // Calculate min, max, average, and sort for percentiles
                double min_latency = get_latencies_us[0];
                double max_latency = get_latencies_us[0];
                double sum_latency = 0;
                
                for (double lat : get_latencies_us) {
                    if (lat < min_latency) min_latency = lat;
                    if (lat > max_latency) max_latency = lat;
                    sum_latency += lat;
                }
                double avg_latency = sum_latency / get_latencies_us.size();

                // Sort for percentiles
                std::vector<double> sorted_latencies = get_latencies_us;
                std::sort(sorted_latencies.begin(), sorted_latencies.end());
                
                size_t n = sorted_latencies.size();
                double p50 = sorted_latencies[n / 2];
                double p90 = sorted_latencies[(size_t)(n * 0.9)];
                double p99 = sorted_latencies[(size_t)(n * 0.99)];

                std::cout << std::fixed << std::setprecision(2);
                std::cout << "Min latency: " << min_latency << " us" << std::endl;
                std::cout << "Max latency: " << max_latency << " us" << std::endl;
                std::cout << "Avg latency: " << avg_latency << " us" << std::endl;
                std::cout << "P50 latency: " << p50 << " us" << std::endl;
                std::cout << "P90 latency: " << p90 << " us" << std::endl;
                std::cout << "P99 latency: " << p99 << " us" << std::endl;
            }
        }

        // Flush logs on all shards in subgroup 0 of PersistentCascadeStoreWithStringKey
        std::cout << std::endl;
        std::cout << "=== Flushing logs on all shards ===" << std::endl;
        {
            auto shards = capi.get_subgroup_members<PersistentCascadeStoreWithStringKey>(subgroup_index);
            std::cout << "  Sending flush_log to " << shards.size() << " shards in subgroup " << subgroup_index << " ..." << std::endl;
            
            ObjectWithStringKey flush_obj;
            std::string flush_value = "flush";
            flush_obj.blob = Blob(reinterpret_cast<const uint8_t*>(flush_value.c_str()), flush_value.size());
            flush_obj.previous_version = persistent::INVALID_VERSION;
            flush_obj.previous_version_by_key = persistent::INVALID_VERSION;
            flush_obj.set_message_id(10000001);
            uint32_t shard_id = 0;
            for ([[maybe_unused]] auto& shard : shards) {
                flush_obj.key = "/flush_log/" + std::to_string(shard_id);
                std::cout << "    Sending flush to shard " << shard_id << " with key: " << flush_obj.key << std::endl;
                
                // Send put to each node in the shard to ensure all replicas receive it
                
                // Each iteration reaches a different node in the shard due to round robin policy
                auto res = capi.template put<PersistentCascadeStoreWithStringKey>(flush_obj, subgroup_index, shard_id, true);
                for (auto& reply_future : res.get()) {
                    reply_future.second.get(); // Wait for the put to complete
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
