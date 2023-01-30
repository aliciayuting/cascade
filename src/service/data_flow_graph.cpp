#include <derecho/utils/logger.hpp>
#include <cascade/data_flow_graph.hpp>
#include <iostream>
#include <fstream>
#include <cascade/service.hpp>

namespace derecho {
namespace cascade {

DataFlowGraph::DataFlowGraph():id(""),description("uninitialized DFG") {}

DataFlowGraph::DataFlowGraph(const json& dfg_conf):
    id(dfg_conf[DFG_JSON_ID]),
    description(dfg_conf[DFG_JSON_DESCRIPTION]) {
    for(auto it=dfg_conf[DFG_JSON_GRAPH].cbegin();it!=dfg_conf[DFG_JSON_GRAPH].cend();it++) {
        DataFlowGraphVertex dfgv;
        dfgv.pathname = (*it)[DFG_JSON_PATHNAME];
        /* fix the pathname if it is not ended by a separator */
        if(dfgv.pathname.back() != PATH_SEPARATOR) {
            dfgv.pathname = dfgv.pathname + PATH_SEPARATOR;
        }
        for(size_t i=0;i<(*it)[DFG_JSON_UDL_LIST].size();i++) {
            std::string udl_uuid = (*it)[DFG_JSON_UDL_LIST].at(i);
            // shard dispatchers
            dfgv.shard_dispatchers[udl_uuid] = DataFlowGraph::VertexShardDispatcher::ONE;
            if (it->contains(DFG_JSON_SHARD_DISPATCHER_LIST)) {
                dfgv.shard_dispatchers[udl_uuid] = ((*it)[DFG_JSON_SHARD_DISPATCHER_LIST].at(i).get<std::string>() == "all")?
                    DataFlowGraph::VertexShardDispatcher::ALL : DataFlowGraph::VertexShardDispatcher::ONE;
            }
            // stateful
            dfgv.stateful[udl_uuid] = DataFlowGraph::Statefulness::STATEFUL;
            if (it->contains(DFG_JSON_UDL_STATEFUL_LIST)) {
                if ((*it)[DFG_JSON_UDL_STATEFUL_LIST].at(i).get<std::string>() == "stateless") {
                    dfgv.stateful[udl_uuid] = DataFlowGraph::Statefulness::STATEFUL;
                } else if ((*it)[DFG_JSON_UDL_STATEFUL_LIST].at(i).get<std::string>() == "singlethreaded") {
                    dfgv.stateful[udl_uuid] = DataFlowGraph::Statefulness::SINGLETHREADED;
                }
            }
            // hooks
            dfgv.hooks[udl_uuid] = DataFlowGraph::VertexHook::BOTH;
            if (it->contains(DFG_JSON_UDL_HOOK_LIST)) {
                if ((*it)[DFG_JSON_UDL_HOOK_LIST].at(i).get<std::string>() == "trigger") {
                    dfgv.hooks[udl_uuid] = DataFlowGraph::VertexHook::TRIGGER_PUT;
                } else if ((*it)[DFG_JSON_UDL_HOOK_LIST].at(i).get<std::string>() == "ordered") {
                    dfgv.hooks[udl_uuid] = DataFlowGraph::VertexHook::ORDERED_PUT;
                }
            }
            // configurations
            if (it->contains(DFG_JSON_UDL_CONFIG_LIST)) {
                dfgv.configurations.emplace(udl_uuid,(*it)[DFG_JSON_UDL_CONFIG_LIST].at(i));
            } else {
                dfgv.configurations.emplace(udl_uuid,json{});
            }
            // information for scheduler
            // required_model_list
            if (it->contains(DFG_JSON_REQUIRED_MODELS_LIST)){
                dfgv.required_models.emplace_back((*it)[DFG_JSON_REQUIRED_MODELS_LIST].at(i).get<uint32_t>());
            } else{
                dfgv.required_models.emplace_back(0);
            }
            // required_model_size_list
            if (it->contains(DFG_JSON_REQUIRED_MODELS_SIZE_LIST)){
                dfgv.required_models_size.emplace_back((*it)[DFG_JSON_REQUIRED_MODELS_SIZE_LIST].at(i).get<uint32_t>());
            } else{
                dfgv.required_models_size.emplace_back(0);
            }
            // expected_output_size in KB
            if (it->contains(DFG_JSON_UDL_OUTPUT_SIZE_LIST)){
                dfgv.expected_output_size.emplace(udl_uuid, (*it)[DFG_JSON_UDL_OUTPUT_SIZE_LIST].at(i).get<uint32_t>());
            } else{
                dfgv.expected_output_size.emplace(udl_uuid, 0);
            }
            // expected_gpu_execution_timeus
            if (it->contains(DFG_JSON_EXEC_TIME_LIST)){
                dfgv.expected_execution_timeus.emplace(udl_uuid, (*it)[DFG_JSON_EXEC_TIME_LIST].at(i).get<uint64_t>());
            } else{
                dfgv.expected_execution_timeus.emplace(udl_uuid, 0);
            }
            // edges: next steps edges
            std::map<std::string,std::string> dest = 
                (*it)[DFG_JSON_DESTINATIONS].at(i).get<std::map<std::string,std::string>>();

            if (dfgv.edges.find(udl_uuid) == dfgv.edges.end()) {
                dfgv.edges.emplace(udl_uuid,std::unordered_map<std::string,bool>{});
            }
            for(auto& kv:dest) {
                if (kv.first.back() == PATH_SEPARATOR) {
                    dfgv.edges[udl_uuid].emplace(kv.first,(kv.second==DFG_JSON_TRIGGER_PUT)?true:false);
                } else {
                    dfgv.edges[udl_uuid].emplace(kv.first + PATH_SEPARATOR,(kv.second==DFG_JSON_TRIGGER_PUT)?true:false);
                }
            }
        }
        // required objects' pathnames
        for(size_t i=0;i<(*it)[DFG_REQUIRED_OBJECTS_LIST].size();i++) {
            std::string objects_pathname = (*it)[DFG_REQUIRED_OBJECTS_LIST].at(i);
            /* fix the pathname if it is not ended by a separator */
            if(objects_pathname.back() != PATH_SEPARATOR) {
                objects_pathname = objects_pathname + PATH_SEPARATOR;
            }
            dfgv.required_objects_pathnames.emplace(objects_pathname);
        }
        vertices.emplace(dfgv.pathname,dfgv);
    }
    // Helper function for scheduler: rank the verticies by their dependencies.
    std::vector<std::string> ranked_verticies;
    std::vector<std::string> next_verticies_to_process;
    for(auto vertex:vertices){
        if(vertex.second.required_objects_pathnames.empty()){
            next_verticies_to_process.emplace_back(vertex.first);
        }
    }
    while(!next_verticies_to_process.empty()){
        std::string proc_vertex_pathname = next_verticies_to_process.back();
        next_verticies_to_process.pop_back();
        if(std::find(ranked_verticies.begin(), ranked_verticies.end(), proc_vertex_pathname) == ranked_verticies.end()){
            ranked_verticies.emplace_back(proc_vertex_pathname);
        }
        const auto& vertex = vertices.at(proc_vertex_pathname);
        for(const auto& edge: vertex.edges){
            auto pathname = edge.first;
            // check if required verticies are in the ranked_verticies already
            bool ranked_all_required = true;
            for(auto& required_pathname: vertices.at(pathname).required_objects_pathnames){
                ranked_all_required = std::find(ranked_verticies.begin(), ranked_verticies.end(), required_pathname) != ranked_verticies.end();
                if(!ranked_all_required){
                    break;
                }
            }
            if(ranked_all_required){
                next_verticies_to_process.emplace_back(pathname);
            }
            
        }
    }
    this->sorted_pathnames = ranked_verticies;
}

DataFlowGraph::DataFlowGraph(const DataFlowGraph& other):
    id(other.id),
    description(other.description),
    vertices(other.vertices) {}

DataFlowGraph::DataFlowGraph(DataFlowGraph&& other):
    id(other.id),
    description(other.description),
    vertices(std::move(other.vertices)) {}

void DataFlowGraph::dump() const {
    std::cout << "DFG: {\n"
              << "id: " << id << "\n"
              << "description: " << description << "\n";
    for (auto& kv:vertices) {
        std::cout << kv.second.to_string() << std::endl;
    }
    std::cout << "}" << std::endl;
}

DataFlowGraph::~DataFlowGraph() {}

std::vector<DataFlowGraph> DataFlowGraph::get_data_flow_graphs() {
    std::ifstream i(DFG_JSON_CONF_FILE);
    if (!i.good()) {
        dbg_default_warn("{} is not found.", DFG_JSON_CONF_FILE);
        return {};
    }

    json dfgs_json;
    i >> dfgs_json;
    //TODO: validate dfg.
    std::vector<DataFlowGraph> dfgs;
    for(auto it = dfgs_json.cbegin();it != dfgs_json.cend(); it++) {
        dfgs.emplace_back(DataFlowGraph(*it));
    }
    return dfgs;
}

}
}
