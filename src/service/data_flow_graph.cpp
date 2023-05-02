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
        // information for scheduler
        TaskInfo task_info;
        std::vector<MLModelInfo> models_info;
        if (it->contains(DFG_JSON_REQUIRED_MODELS)){
            for (size_t i = 0; i < (*it)[DFG_JSON_REQUIRED_MODELS].size(); i++){
                MLModelInfo model_info;
                auto& dfg_model_info = (*it)[DFG_JSON_REQUIRED_MODELS].at(i);
                model_info.model_id = dfg_model_info[DFG_JSON_MODEL_ID].get<uint32_t>();
                // model size in KB
                model_info.model_size = dfg_model_info[DFG_JSON_MODEL_SIZE].get<uint32_t>();
                task_info.models_info.emplace_back(model_info);
            }
        } 
        // input_size in KB
        if (it->contains(DFG_JSON_UDL_INPUT_SIZE)){
            task_info.input_size = (*it)[DFG_JSON_UDL_INPUT_SIZE].get<uint32_t>();
        } 
        // output_size in KB
        if (it->contains(DFG_JSON_UDL_OUTPUT_SIZE)){
            task_info.output_size = (*it)[DFG_JSON_UDL_OUTPUT_SIZE].get<uint32_t>();
        } 
        
        if (it->contains(DFG_JSON_EXEC_TIME)){
            task_info.expected_execution_timeus = (*it)[DFG_JSON_EXEC_TIME].get<uint64_t>();
        } 
        // required objects' pathnames
        if (it->contains(DFG_REQUIRED_OBJECTS_LIST)){
            for(size_t i = 0; i < (*it)[DFG_REQUIRED_OBJECTS_LIST].size(); i++) {
                std::string objects_pathname = (*it)[DFG_REQUIRED_OBJECTS_LIST].at(i);
                /* fix the pathname if it is not ended by a separator */
                if(objects_pathname.back() != PATH_SEPARATOR) {
                    objects_pathname = objects_pathname + PATH_SEPARATOR;
                }
                task_info.required_objects_pathnames.emplace_back(objects_pathname);
            }
        }
        dfgv.task_info = task_info;
        vertices.emplace(dfgv.pathname,dfgv);
    }
    // Helper function for scheduler: rank the verticies by their dependencies.
    std::vector<std::string> next_verticies_to_process;
    for(auto& vertex:vertices){
        if(vertex.second.task_info.required_objects_pathnames.empty()){
            next_verticies_to_process.emplace_back(vertex.first);
        }
    }
    while(!next_verticies_to_process.empty()){
        std::string proc_vertex_pathname = next_verticies_to_process.back();
        next_verticies_to_process.pop_back();
        bool has_ranked = std::find(this->sorted_pathnames.begin(), this->sorted_pathnames.end(), proc_vertex_pathname) != this->sorted_pathnames.end();
        // check if required verticies are in the ranked_verticies already
        bool ranked_all_required = true;
        for(auto& required_pathname: vertices.at(proc_vertex_pathname).task_info.required_objects_pathnames){
            ranked_all_required = std::find(this->sorted_pathnames.begin(), this->sorted_pathnames.end(), required_pathname) != this->sorted_pathnames.end();
            if(!ranked_all_required){
                break;
            }
        }
        if(!has_ranked && ranked_all_required ){
            this->sorted_pathnames.emplace_back(proc_vertex_pathname);
        }
        const auto& vertex = vertices.at(proc_vertex_pathname);
        for(const auto& uuid_map: vertex.edges){
            for(const auto& edge: uuid_map.second){
                auto pathname = edge.first;  
                if( std::find(this->sorted_pathnames.begin(), this->sorted_pathnames.end(), pathname) == this->sorted_pathnames.end()){
                    next_verticies_to_process.emplace_back(pathname);
                }        
            }
        }   
    }
}

DataFlowGraph::DataFlowGraph(const DataFlowGraph& other):
    id(other.id),
    description(other.description),
    vertices(other.vertices),
    sorted_pathnames(other.sorted_pathnames) {}

DataFlowGraph::DataFlowGraph(DataFlowGraph&& other):
    id(other.id),
    description(other.description),
    vertices(std::move(other.vertices)),
    sorted_pathnames(std::move(other.sorted_pathnames)) {}

void DataFlowGraph::dump() const {
    std::cout << "DFG: {\n"
              << "id: " << id << "\n"
              << "description: " << description << "\n";
    for (auto& kv:vertices) {
        std::cout << kv.second.to_string() << std::endl;
    }
    std::cout << "sorted_pathnames:";
    for (auto& pathname:sorted_pathnames){
        std::cout << pathname << ",";
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
