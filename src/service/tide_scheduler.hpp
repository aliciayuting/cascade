#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <cascade/service.hpp>
#include <cascade/utils.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>


namespace derecho{
namespace cascade{

#define FIXED_COST_GROUP_FORMATION  (2)

uint64_t round_double(double _d){
     if (_d >= -9223372036854775808.0   // -2^63
          && _d <   9223372036854775808.0)  // 2^63
     {
          return static_cast<uint64_t>(_d);
     }
     return UINT64_MAX;
}

/**
 * Estimated time(us) to move object from host machine to GPU, using the data collected
 * @param object_size     the size of the object to move, in KB
*/
uint64_t host_to_GPU_delay(uint64_t object_size){
     double throughput = 4.7 * (1 << 20) / 1000.0;
     double delay = (1000.0 * object_size / throughput) + FIXED_COST_GROUP_FORMATION;
     return round_double(delay);
}

/**
 * Estimated time(us) to send object over RDMA 
 * @param object_size     the size of the object to move, in KB
*/
uint64_t CPU_to_CPU_delay(uint64_t object_size){
     double throughput;
     double delay;
     if(object_size < 1){
          return 2; // 2 us
     }else if(object_size < (1 << 2)){ // for size between 1 ~ 4 KB
          throughput = (3 + object_size * 2) * (1 << 20) / 1000.0;
          delay = (object_size / throughput) + FIXED_COST_GROUP_FORMATION;
     }else{
          throughput = 12 * (1 << 20) / 1000.0;
          delay = (object_size / throughput) + FIXED_COST_GROUP_FORMATION;
     }
     return round_double(delay);
     
}

/**
 * Estimated time(us) to move object from GPU to host machine, using the data collected
 * @param object_size     the size of the object to move, in KB
*/
uint64_t GPU_to_host_delay(uint64_t object_size){
     double throughput = 4.7 * (1 << 20) / 1000.0;
     double delay = (object_size / throughput) + FIXED_COST_GROUP_FORMATION;
     return round_double(delay);
}

/**
 * Estimated time(us) to move object between GPU's on different servers
 * Note: this could be smaller after implement GPU to GPU direct feature
 * @param object_size     the size of the object to move, in KB
*/
uint64_t GPU_to_GPU_delay(uint64_t object_size){
     uint64_t localgpu_to_localcpu_delay = GPU_to_host_delay(object_size);
     uint64_t cpu_to_cpu_delay = CPU_to_CPU_delay(object_size);
     uint64_t remotecpu_to_remotegpu_delay = host_to_GPU_delay(object_size);
     return localgpu_to_localcpu_delay + cpu_to_cpu_delay + remotecpu_to_remotegpu_delay;
}

/**
 * given a prefix of the first task of the dfg generage the adfg result for this instance
 * @param node_id   local node_id
 * @param ctxt      CascadeContext
*/
template <typename... CascadeTypes>
std::string tide_schedule_dfg(const node_id_t node_id, std::string entry_prefix, CascadeContext<CascadeTypes...>* ctxt){
     // vertex pathname -> (node_id, finish_time(us))
     // in the algorithm denote task ~ vertex pathname
     std::unordered_map<std::string, std::tuple<node_id_t,uint64_t>> allocated_tasks_info;
     std::set<node_id_t> workers_set = ctxt.get_service_client_ref().get_members();
     pre_adfg_t pre_adfg = ctxt->get_pre_adfg(entry_prefix);
     uint64_t cur_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::high_resolution_clock::now().time_since_epoch())
                              .count();
     /** TODO: optimize this get_sorted_pathnames step by better design of where to store sorted_pathnames in pre_adfg_t */
     std::vector<std::string>& sorted_pathnames;
     for(auto& item: handlers){
          sorted_pathnames = std::get<3>(item.second); 
          break;   
     }
     for(auto& pathname: sorted_pathnames){
          auto& dependencies = pre_adfg.at(pathname);
          // 0. PRE-COMPUTE (used later by 2. case1) get the earliest start time, suppose all preq_tasks need to transfer data
          uint64_t prev_EST = cur_us;
          // the worker_ids where pre-requisit tasks are executed
          std::set<node_id_t> preq_workers;
          std::set<std::string>& required_tasks = std::get<0>(dependencies);
          for(auto& preq_task : required_tasks){
               preq_workers.emplace(std::get<0>(allocated_tasks_info.at(preq_task)));
               uint64_t preq_finish_time = std::get<0>(allocated_tasks_info.at(preq_task));
               uint32_t preq_result_size = std::get<4>(pre_adfg.at(pathname));
               uint64_t preq_arrive_time = preq_finish_time + GPU_to_GPU_delay(preq_result_size);
               prev_EST = std::max(prev_EST, preq_arrive_time);
          }
          if(pathname == sorted_pathnames[0]){ // first task
               /** TODO: assumming input_size=output_size, for finer-grained HEFT, use input size instead of output size*/
               prev_EST += host_to_GPU_delay(std::get<4>(pre_adfg.at(pathname))); 
          }
          std::map<node_id_t, uint64_t> workers_start_times;
          for(node_id_t cur_worker: workers_set){
               uint64_t cur_worker_waittime = 0;
               uint64_t model_fetch_time = 0;
               cur_worker_waittime = ctxt.check_queue_wait_time(cur_worker);
               bool models_in_cache;
               auto& required_models = std::get<1>(pre_adfg.at(pathname));
               auto& required_models_size = std::get<2>(pre_adfg.at(pathname));
               for(size_t idx = 0; idx ++; idx < required_models.size()){
                    models_in_cache = ctxt->check_if_model_in_gpu(cur_worker, required_models[idx]);
                    if(!models_in_cache){
                         /** TODO: current design assume host loaded all models at the beginning.
                          *        later can extend to remote_host_to_GPU, with the udl&model centraliezd store
                         */
                         model_fetch_time = model_fetch_time + host_to_GPU_delay(required_models_size[idx]);
                    }
               } 
               /** case 2.1 cur_woker is not the same worker as any of the pre-req tasks'
                *  input fetching/sending is not blocked by waiting queue, whereas model fetching is
                */
               uint64_t start_time;
               if(preq_workers.find(cur_worker) == preq_workers.end()){
                    start_time = std::max(prev_EST, cur_us + cur_worker_waittime + model_fetch_time);
               }else{//case 2.2 cur_worker is on the same node of one of the pre-req tasks
                    uint64_t preq_arrival_time = 0;
                    for(auto& preq_task : required_tasks){
                         node_id_t& preq_worker = std::get<0>(allocated_tasks_info.at(preq_task));
                         uint64_t& preq_finish_time = std::get<1>(allocated_tasks_info.at(preq_task));
                         if(cur_worker == preq_worker){
                              preq_arrival_time = std::max(preq_arrival_time, preq_finish_time);
                         }else{
                              preq_arrival_time = std::max(preq_arrival_time, preq_finish_time + GPU_to_GPU_delay(std::get<4>(pre_adfg.at(pathname))));
                              start_time = std::max(preq_arrival_time, cur_us + cur_worker_waittime + model_fetch_time);
                         }
                    }
               }
               workers_start_times.emplace(cur_worker,start_time);
          }
          auto it = std::min_element(std::begin(workers_start_times), std::end(workers_start_times),
                                                       [](const auto& l, const auto& r) { return l.second < r.second; });
          /** TODO: TEST THIS!!! https://stackoverflow.com/questions/2659248/how-can-i-find-the-minimum-value-in-a-map */
          node_id_t selected_worker = it->second;
          uint64_t cur_task_finish_time = it->first + std::get<5>(pre_adfg.at(pathname));
          allocated_tasks_info.emplace(std::piecewise_construct, std::forward_as_tuple(pathname), std::forward_as_tuple(selected_worker, cur_task_finish_time));
     }    
     std::string allocated_machines;
     for(auto& pathname: sorted_pathnames){
          allocated_machines +=  std::to_string(std::get<1>(allocated_tasks_info.at(pathname))) + ",";
     }
     std::cout << "Allocated machines are: " << allocated_machines << std::endl;
     return allocated_machines;
}

} // namespace cascade
} // namespace derecho