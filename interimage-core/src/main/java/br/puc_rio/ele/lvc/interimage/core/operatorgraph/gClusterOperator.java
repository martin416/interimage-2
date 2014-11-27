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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.JSONObject;

import br.puc_rio.ele.lvc.interimage.core.clustermanager.ClusterManager;

/**
 * Class that executes an operator on a cluster.<br>
 * Right now this operator is written in Pig, but this can be extended in the future to Hive or other languages.
 * @author Rodrigo Ferreira
 */

public class gClusterOperator extends gOperator {
	
	private PigParser parser_;
	private String operatorName_;
	private Properties properties_;
	private Map<String,String> parameters_;
	private String script_;
	
	public gClusterOperator() {
		parameters_ = new HashMap<String, String>();
		parser_ = new PigParser();
	}
	
	public void setParser(PigParser parser) {
		parser_ = parser;
	}
		
	public void setOperatorName(String operatorName) {
		operatorName_ = operatorName;
	}
	
	public void setScript(String script) {
		script_ = script;
	}
	
	public void setProperties(Properties properties) {
		properties_ = properties;
		parser_.setup(properties);
	}
	
	/*public void setParameters(Map<String,String> parameters) {
		parameters_ = parameters;
	}*/
	
	public void setParameter(String key, String value) {
		parameters_.put(key, value);
	}
	
	/** This method must be called after the configuration of the operator and before its execution.*/	
	public void prepare() {		
		parser_.setParams(parameters_);
	}
		
	/** This method executes a Pig script on the given cluster.*/
	@Override
	protected int execute(ClusterManager clusterManager, String clusterId, boolean setup) {
						
		/*Setting input paths from previous nodes output paths*/
		
		/*DefaultDirectedGraph<gNode, gEdge> graph = getGraph();
		
		Set<gEdge> edges = graph.incomingEdgesOf(this);
		
		int count = 1;
		
		for (gEdge edge : edges) {
			gClusterOperator node = (gClusterOperator)graph.getEdgeSource(edge);			
			parameters_.put("$INPUT_PATH_" + String.valueOf(count), node.getOutputPath());
			
			//System.out.println("$INPUT_PATH_" + String.valueOf(count) + ": " + node.getOutputPath());
			
			count++;
		}*/
		
		/*Passing parameters to the parser*/
		prepare();
				
		StringBuilder script = new StringBuilder();
		
		if (setup) {
		
			/*Pig setup*/
			
			/*int tilesPerTask = 0;
			int clusterSize = Integer.parseInt(properties_.getProperty("interimage.clusterSize"));
			int tileRecordSize = Integer.parseInt(properties_.getProperty("interimage.tileRecordSize"));
			
			File folder1 = new File(URL.getPath(properties_.getProperty("interimage.projectPath")) + "tiles/");
			
			int totalSize = folder1.list().length;
			
			tilesPerTask = totalSize / ((clusterSize-1) * 8);*/
			
			script.append("SET pig.tmpfilecompression true;\n");
			script.append("SET pig.tmpfilecompression.codec lzo;\n");
			script.append("SET pig.splitCombination true;\n");
			//script.append("SET pig.maxCombinedSplitSize ").append(tilesPerTask*tileRecordSize).append(";\n");
			
			/*Including JARs*/
			
			File folder2 = new File("lib");
			
			for (final File fileEntry : folder2.listFiles()) {
		        if (fileEntry.isDirectory()) {
		        	//ignore
		        } else {
		        	script.append("REGISTER " + properties_.getProperty("interimage.sourceSpecificURL") + "interimage/lib/" + fileEntry.getName() + ";\n");
		        }
		    }
			
			/*Pig include*/
			
			script.append("IMPORT '" + properties_.getProperty("interimage.sourceSpecificURL") + "interimage/scripts/interimage-import.pig';\n");
			
		}
		
		if (script_ == null)
			script.append(parser_.parse(operatorName_));
		else
			script.append(parser_.parse(script_));
		
		//setOutputPath(parser_.getResult());
		
		System.out.println(script);
		
		//clusterManager.runPigScript(script.toString(), clusterId);
		
		return 0;
	}

	public String getOutputPath() {
		return parameters_.get("$OUTPUT_PATH");
	}
	
	@Override
	public JSONObject exportToJSON() {
		//TODO: Implement this
		return null;
	}

	@Override
	public Boolean importFromJSON(JSONObject path) {
		//TODO: Implement this
		return null;
	}

}
