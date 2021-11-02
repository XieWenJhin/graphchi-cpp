
/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
 *
 * @section DESCRIPTION
 *
 * Template for GraphChi applications. To create a new application, duplicate
 * this template.
 */



#include <string>

#include "graphchi_basic_includes.hpp"
#include "util/toplist.hpp"

using namespace graphchi;



vid_t sid = 0;
/**
  * Type definitions. Remember to create suitable graph shards using the
  * Sharder-program. 
  */
typedef vid_t VertexDataType;
typedef vid_t EdgeDataType;
bool converged = true;

/**
  * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
  * class. The main logic is usually in the update function.
  */
struct SSSPProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {
    
 
    /**
     *  Vertex update function.
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {
        if (gcontext.iteration == 0) {
            /* On first iteration, initialize vertex (and its edges). This is usually required, because
               on each run, GraphChi will modify the data files. To start from scratch, it is easiest
               do initialize the program in code. Alternatively, you can keep a copy of initial data files. */
            // vertex.set_data(init_value);
            if(vertex.id() == sid){
                vertex.set_data(0);
                //std::cout << vertex.id() << "\n";
                for(int i = 0; i < vertex.num_outedges(); i++) {
                    vertex.outedge(i)->set_data(0);
                    gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                    //std::cout << vertex.outedge(i)->vertex_id() << " ";
                }
            }else{
                vertex.set_data(INT_MAX);
                for(int i = 0; i < vertex.num_outedges(); i++) {
                    vertex.outedge(i)->set_data(INT_MAX);
                    //std::cout << vertex.outedge(i)->vertex_id() << " ";
                }
            }
            converged = false;
        } else {
            /* Do computation */ 
            //std::cout << "iter: " << gcontext.iteration << " vid:" << vertex.id() << "\n";
            /* Loop over in-edges (example) */
            unsigned int shortest = INT_MAX;
            for(int i=0; i < vertex.num_inedges(); i++) {
                unsigned int dis = vertex.inedge(i)->get_data();
                if(dis < shortest){
                    shortest = dis;
                }
            }
            if(shortest + 1 < vertex.get_data()){
                std::cout << "iter: " << gcontext.iteration << " vid:" << vertex.id() << " value: " << shortest + 1 << "\n";
                converged = false;
                vertex.set_data(shortest + 1);
                /* Loop over out-edges (example) */
                for(int i=0; i < vertex.num_outedges(); i++) {
                    vertex.outedge(i)->set_data(shortest + 1);
                    gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                }
            }
            // /* Loop over all edges (ignore direction) */
            // for(int i=0; i < vertex.num_edges(); i++) {
            //     // vertex.edge(i).get_data() 
            // }
            
            // v.set_data(new_value);
        }
    }
    
    /**
     * Called before an iteration starts.
     */
    void before_iteration(int iteration, graphchi_context &gcontext) {
        converged = true;
        //std::cout << "iter: " << gcontext.iteration << " tasks: " << gcontext.scheduler->num_tasks() << "\n";
    }
    
    /**
     * Called after an iteration has finished.
     */
    void after_iteration(int iteration, graphchi_context &gcontext) {
        if (converged) {
            std::cout << "Converged!" << std::endl;
            std::cout << iteration;
            gcontext.set_last_iteration(iteration);
        }
    }
    
    
};

int main(int argc, const char ** argv) {
    /* GraphChi initialization will read the command line 
       arguments and the configuration file. */
    graphchi_init(argc, argv);
    
    /* Metrics object for keeping track of performance counters
       and other information. Currently required. */
    metrics m("sssp");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int niters           = get_option_int("niters", 10000); // Number of iterations
    bool scheduler       = get_option_int("scheduler", true); // Whether to use selective scheduling
    sid                  = get_option_int("sid", 0);
    
    /* Detect the number of shards or preprocess an input to create them */
    int nshards          = convert_if_notexists<EdgeDataType>(filename, 
                                                            get_option_string("nshards", "auto"));
    
    /* Run */
    SSSPProgram program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m); 
    engine.run(program, niters);
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
