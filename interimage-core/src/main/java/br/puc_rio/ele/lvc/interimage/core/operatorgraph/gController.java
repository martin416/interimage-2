/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.util.*;

import org.jgrapht.graph.*;
import org.json.*;

import java.util.HashMap;
import java.util.Map;

import br.puc_rio.ele.lvc.interimage.core.clustermanager.AWSClusterManager;
import br.puc_rio.ele.lvc.interimage.core.clustermanager.ClusterManager;

import com.mortardata.api.v2.Jobs;

/**
 * Class that manages the operator graph. 
 * @author Dário Oliveira, Rodrigo Ferreira
 */

public class gController extends DefaultDirectedGraph<gNode, gEdge> {

	private static final long serialVersionUID = 8285085270334519490L;
	public Jobs mortarJobs_;
	private ClusterManager clusterManager_;
	private boolean setup_;
	@SuppressWarnings("unused")
	private Properties properties_;
	
	public gController() {
		super(gEdge.class);
		
		clusterManager_ = new AWSClusterManager();
		setup_=true;
		
		// TODO Auto-generated constructor stub
	}

	public void setProperties(Properties props) {
		properties_ = props;
		clusterManager_.setProperties(props);
	}
	
	public String exportToJSON(){
		
		Set<gNode> nodes = this.vertexSet();
		JSONObject my_obj_ = new JSONObject();
		
		JSONArray nodesJS = new JSONArray();
		for (gNode node : nodes )
		{
			JSONObject my_obj = new JSONObject();
			
			my_obj.put("id", node.getNodeId());
			my_obj.put("properties", node.exportToJSON());

			nodesJS.put(my_obj); 

			my_obj=null;
			
		}
		
		my_obj_.put("nodes", nodesJS);
		
		Set<gEdge> edges = this.edgeSet();
		
		JSONArray edgesJS = new JSONArray();
		for (gEdge edge : edges )
		{
			JSONObject my_obj = new JSONObject();
			
			my_obj.put("fromNode", this.getEdgeSource(edge).getNodeId() );
			my_obj.put("toNode", this.getEdgeTarget(edge).getNodeId());
			my_obj.put("properties", edge.exportToJSON());
			
			edgesJS.put(my_obj); 

			my_obj=null;
		}
		
		my_obj_.put("edges", edgesJS);
		
		return my_obj_.toString();
	}
	
	public Jobs getMortarJobs_() {
		return mortarJobs_;
	}

	public void setMortarJobs_(Jobs mortarJobs_) {
		this.mortarJobs_ = mortarJobs_;
	}

	public void importFromJSON(String json_string){
		Map<Long,gNode> map_ = new HashMap<Long,gNode>();
		
		JSONObject my_obj = new JSONObject(json_string);
		
		JSONArray nodes = new JSONArray(my_obj.get("nodes"));
		JSONArray edges = new JSONArray(my_obj.get("edges"));
		
		my_obj=null;
		
		for (int i = 0; i < nodes.length(); i++) {
			JSONObject my_node = new JSONObject(nodes.get(i));
			JSONObject my_op = (new JSONObject(my_node.getString("properties")));
			
			if (my_op.get("type")=="gMortarOperator")
			{
				gMortarOperator o = this.addMortarOperator(my_op);
				map_.put(my_node.getLong("id"), o);
			} else if (my_op.get("type")=="gCommandLineOperator")
			{
				gCommandLineOperator o = this.addCommandLineOperator(my_op);
				map_.put(my_node.getLong("id"), o);
			} else if (my_op.get("type")=="gII1Operator")
			{
				gII1Operator o = this.addII1Operator(my_op);
				map_.put(my_node.getLong("id"), o);	
			}
			my_node=null;
		}
		
		for (int i = 0; i < edges.length(); i++) {
			JSONObject my_edge = new JSONObject(edges.get(i));
			this.addEdge(map_.get(my_edge.getLong("fromNode")), map_.get(my_edge.getLong("toNode")));
			my_edge=null;
		}
		
	}
	
	//update inputs from successor nodes with output from node
	public void updateLinkedNodes(gNode node){
		Set<gEdge> relatedEdges = this.outgoingEdgesOf(node);
		for (gEdge outgoing : relatedEdges )
		{
			if (outgoing.isActivated())
				this.getEdgeTarget(outgoing).improveRequest();
		}
		
	}
	
	//evaluate the input data and the requests for executing 
	public void evaluateNodeInputs(gNode node){
		if (node.getRequests()==this.inDegreeOf(node))
			if (!node.isExecuted())
				node.setAvailable(true);
	}

	public gMortarOperator addMortarOperator(JSONObject obj){
		gMortarOperator node = new gMortarOperator();
		node.setJobs_(mortarJobs_);
		node.importFromJSON(obj);
		this.addVertex(node);
		return node;
	}
	public gMortarOperator addMortarOperator(String scriptPath){
		gMortarOperator node = new gMortarOperator(scriptPath);
		node.setJobs_(mortarJobs_);
		this.addVertex(node);
		return node;
	}
	
	public gCommandLineOperator addCommandLineOperator(String command){
		gCommandLineOperator node = new gCommandLineOperator(command);
		this.addVertex(node);
		return node;
	}
	public gCommandLineOperator addCommandLineOperator(JSONObject obj){
		gCommandLineOperator node = new gCommandLineOperator();
		node.importFromJSON(obj);
		this.addVertex(node);
		return node;
	}
	
	public gII1Operator addII1Operator(){
		gII1Operator node = new gII1Operator();
		this.addVertex(node);
		return node;
	}
	public gII1Operator addII1Operator(JSONObject obj){
		gII1Operator node = new gII1Operator();
		node.importFromJSON(obj);
		this.addVertex(node);
		return node;
	}
	
	public gClusterOperator addClusterOperator() {
		gClusterOperator node = new gClusterOperator();
		this.addVertex(node);
		return node;
	}
	
	public gClusterOperator addClusterOperator(JSONObject obj){
		gClusterOperator node = new gClusterOperator();
		node.importFromJSON(obj);
		this.addVertex(node);
		return node;
	}
	
	//run controller
	public int run(String clusterId)
	{
				
		System.out.println("Controller: run");
		
		int numberOfRunningNodes=0;
		Set<gNode> nodes = this.vertexSet();
		
		for (gNode node : nodes )
		{
			if (node.isRunning())
			{
				
				System.out.println("Controller: node running");
				
				numberOfRunningNodes=numberOfRunningNodes+1;
				continue;
			}

			if (node.isExecuted())
			{
				
				System.out.println("Controller: executed");
				
				updateLinkedNodes(node);	
				continue;
			}
			
			evaluateNodeInputs(node);
			
			if (node.isAvailable())
			{
				
				System.out.println("Controller: available");
				
				//call node run method
				node.run(clusterManager_, clusterId, setup_);
				
				numberOfRunningNodes=numberOfRunningNodes+1;
				
				/*Setup for libs and hadoop/pig in the first run*/
				if (setup_)
					setup_ = false;
				
			}

		}
		return numberOfRunningNodes;
	}
	
	public int execute() {
		
		String clusterId = clusterManager_.createCluster();
		
		try {
			
			int runningNodes = 999;
			
			while (true/*runningNodes > 0*/) {
			    runningNodes = run(clusterId);
			    //System.out.println("Step");
			    //System.out.println(runningNodes);
			    Thread.sleep(10000);			    
			}
			
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		
		//clusterManager_.terminateCluster(clusterId);
		
		return 0;
	}
	
}
