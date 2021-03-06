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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import br.puc_rio.ele.lvc.interimage.common.URL;

/**
 * A class that parses an extended Pig script. 
 * @author Rodrigo Ferreira
 */
public class PigParser {
			
	private String _projectPath;
	private int _parallel;
	private int _cores;
	private int _clusterSize;
	
	private Map<String,String> _export;
	
	private Map<String,String> _params;
	
	private Map<String,String> _specificParams;
	
	private Map<String, List<String>> _relationMap;
	private List<String> _relations;
	
	private Map<String, List<String>> _globalRelationMap;
	private List<String> _globalRelations;
	
	private OperatorSet _set;
	
	private String _sourceURL;
	private String _sourceSpecificURL;
	private String _projectName;
	private String _crs;
	private String _tileSizeMeters;
	private String _minResolution;
	
	public void setup(Properties properties) {
		
		_projectPath = properties.getProperty("interimage.projectPath");				
		_sourceURL = properties.getProperty("interimage.sourceURL");
		_sourceSpecificURL = properties.getProperty("interimage.sourceSpecificURL");
		_projectName = properties.getProperty("interimage.projectName");
		_parallel = Integer.parseInt(properties.getProperty("interimage.parallel"));
		_cores = Integer.parseInt(properties.getProperty("interimage.cores"));
		_clusterSize = Integer.parseInt(properties.getProperty("interimage.clusterSize"));
		_crs = properties.getProperty("interimage.crs");
		_tileSizeMeters = properties.getProperty("interimage.tileSizeMeters");
		_minResolution = properties.getProperty("interimage.minResolution");
		_params = new HashMap<String,String>();
				
		//Random randomGenerator = new Random();
		
		//_params.put("$OUTPUT_PATH", _sourceSpecificURL + "interimage/" + _projectName + "/results/" + randomGenerator.nextInt(100000));
		_params.put("$IMAGES_PATH", _sourceURL + "interimage/" + _projectName + "/resources/images/");
		_params.put("$SHAPES_PATH", _sourceURL + "interimage/" + _projectName + "/resources/shapes/");
		//_params.put("$SHAPES_KEY", "interimage/" + projectName + "/resources/shapes/");
		_params.put("$TILES_FILE", _sourceURL + "interimage/" + _projectName + "/resources/tiles.ser");
		_params.put("$FUZZYSETS_FILE", _sourceURL + "interimage/" + _projectName + "/resources/fuzzysets.ser");
		_params.put("$SEMANTICNET_FILE", _sourceURL + "interimage/" + _projectName + "/resources/semanticnetwork.ser");
		_params.put("$TILES_PATH", _sourceSpecificURL + "interimage/" + _projectName + "/tiles/");
		//_params.put("$DUMP_PATH", _sourceSpecificURL + "interimage/" + _projectName + "/dump/" + randomGenerator.nextInt(100000));
		//_params.put("$RESULTS_PATH", sourceSpecificURL + "interimage/" + projectName + "/results/" + randomGenerator.nextInt(100000));
		_params.put("$TILE_SIZE_METERS", _tileSizeMeters);
		_params.put("$MIN_RESOLUTION", _minResolution);
		_params.put("$PARALLEL", String.valueOf(_parallel));
		_params.put("$CRS", _crs);
		
	}
	
	public PigParser() {
		_relationMap = new HashMap<String, List<String>>();
		_relations = new ArrayList<String>();
		_specificParams = new HashMap<String,String>();
		_export = new HashMap<String,String>();
		_globalRelationMap = new HashMap<String, List<String>>();
		_globalRelations = new ArrayList<String>();
		
		_set = new OperatorSet();
		_set.loadOperators();
	}
	
	public void setParams(Map<String, String> params) {
		_specificParams.clear();
		for (Map.Entry<String, String> entry : params.entrySet()) {
			_specificParams.put(entry.getKey(), entry.getValue());
		}
				
		/*Random randomGenerator = new Random();
		
		//_params.put("$LAST_OUTPUT_PATH", _params.get("$OUTPUT_PATH"));
		//_params.put("$LAST_DUMP_PATH", _params.get("$DUMP_PATH"));
		_params.put("$OUTPUT_PATH", _sourceSpecificURL + "interimage/" + _projectName + "/results/" + randomGenerator.nextInt(100000));
		_params.put("$DUMP_PATH", _sourceSpecificURL + "interimage/" + _projectName + "/dump/" + randomGenerator.nextInt(100000));*/
		
	}
	
	private String replace(String line) {
				
		for (Map.Entry<String, String> entry : _params.entrySet()) {
			if (line.contains(entry.getKey())) {
				line = line.replace(entry.getKey(), entry.getValue());
			}
		}
		
		for (Map.Entry<String, String> entry : _specificParams.entrySet()) {
			if (line.contains(entry.getKey())) {
				line = line.replace(entry.getKey(), entry.getValue());
			}
		}
		
		if (line.contains("$LAST_UNION")) {
    		List<String> list = _globalRelationMap.get("union");
    		line = line.replace("$LAST_UNION",list.get(list.size()-1));
		}
		
		if (line.contains("$LAST_RELATION")) {
			
			String last = _globalRelations.get(_globalRelations.size()-1);
    		line = line.replace("$LAST_RELATION",last);
			
		}
		
		if (line.contains("$B_LAST_RELATION")) {
			
			String last = _globalRelations.get(_globalRelations.size()-2);
    		line = line.replace("$B_LAST_RELATION",last);
			
		}
		
		if (line.contains("$LAST_GROUP")) {
			
			String last = _globalRelations.get(_globalRelations.size()-1);
    		line = line.replace("$LAST_GROUP",last);
			
		}
		
		if (line.contains("$BB_LAST_RELATION")) {
			
			String last = _globalRelations.get(_globalRelations.size()-3);
    		line = line.replace("$BB_LAST_RELATION",last);
			
		}
		
		if (line.contains("$LAST_PROJECTION")) {
    		List<String> list = _globalRelationMap.get("projection");
    		line = line.replace("$LAST_PROJECTION",list.get(list.size()-1));
		}
				
		return line;
		
	}
	
	private String[] evaluate(String line) {
		
		String[] result = new String[2];
		
		String relation = null;
				
		int index = line.indexOf("=");
		
		String[] terms = new String[2];
		terms[0] = line.substring(0,index).trim();
		terms[1] = line.substring(index+1).trim();
		if (_relationMap.containsKey(terms[0])) {
			List<String> list = _relationMap.get(terms[0]);
			List<String> list2 = _globalRelationMap.get(terms[0]);
			int count = list2.size();
			relation = terms[0] + "_" + String.valueOf(count+1);
			list.add(relation);
			list2.add(relation);
			_relations.add(relation);
			_globalRelations.add(relation);
		} else {
			if (_globalRelationMap.containsKey(terms[0])) {
				List<String> list = new ArrayList<String>();
				List<String> list2 = _globalRelationMap.get(terms[0]);
				int count = list2.size();
				relation = terms[0] + "_" + String.valueOf(count+1);
				list.add(relation);
				list2.add(relation);
				_relationMap.put(terms[0], list);				
				_relations.add(relation);
				_globalRelations.add(relation);
			} else {
				List<String> list = new ArrayList<String>();
				List<String> list2 = new ArrayList<String>();
				relation = terms[0] + "_" + String.valueOf(1);				
				list.add(relation);		        			
				list2.add(relation);
				_relationMap.put(terms[0], list);
				_globalRelationMap.put(terms[0], list2);
				_relations.add(relation);
				_globalRelations.add(relation);
			}
		}
		
		result[0] = relation;
		result[1] = terms[1];
		
		return result;
		
	}
	
	private String process(BufferedReader buff, String line, String parsedScript) {
		
		try {
		
			if (line.contains("BEGIN FOR TILES")) {
				
	    		String tiles = "";
	    		
	    		List<String> statements = new ArrayList<String>();
	    		
	    		line = buff.readLine();
	    		
	    		while (!line.contains("END FOR TILES")) {
					statements.add(line);
	    			line = buff.readLine();
	    		}
	    			        		
	    		File folder = new File(URL.getPath(_projectPath) + "tiles/");
					
	    		boolean first1 = true;
	    		boolean first2 = true;
	    			    		
	    		int totalSize = folder.list().length;
	    			    		
	    		int mapSlotsPerTasktracker = (int)Math.round(_cores*2);
	    		
	    		int blockSize = (int)Math.ceil(totalSize / ((double)mapSlotsPerTasktracker*(_clusterSize-1)));
	    			    		
	    		int count1 = 1;
	    		int count2 = 1;	        		
	    		
	    		String names = "{";
	    		
				for (final File fileEntry : folder.listFiles()) {
					
					if (fileEntry.isDirectory()) {
			        	//ignore
			        } else {
			        	
			        	if ((count1<=blockSize) && (count2<=totalSize)) {
			        		
			        		if (first1) {
			        			names = names + fileEntry.getName();
			        			first1 = false;
			        		} else {
			        			names = names + "," + fileEntry.getName();
			        		}
			        		
			        		if ((count1 == blockSize) || (count2 == totalSize)) {
			        			
			        			names = names + "}";
					        	
			        			for (String statement : statements) {
					        		//statement = replace(statement);
					        						        		
					        		statement = statement.replace("$TILE", _params.get("$TILES_PATH") + names);
					        		
					        		/*if (statement.contains("=")) {
					        			String[] result = evaluate(statement);    				        		
						        		parsedScript = parsedScript + result[0] + " = " + result[1] + "\n";
					        		} else {
					        			parsedScript = parsedScript + statement + "\n";
					        		}*/
					        		
					        		parsedScript = process(buff, statement, parsedScript);
		
			        			}
				        	
					        	List<String> list = _relationMap.get("projection");
					        	if (first2) {
					        		tiles = list.get(list.size()-1);
					        		first2 = false;
					        	} else {
					        		tiles = tiles + "," + list.get(list.size()-1);
					        	}
				        						        	
					        	count1 = 0;
					        	
					        	names = "{";
					        	
					        	first1 = true;	
			        							        	
			        		}
			        		
			        		count1++;
				        	count2++;
				        				        		
			        	} 
			        	
			        	
			        }
				}
	    						
				_params.put("$TILES_PROJECTIONS", tiles);
							
	    	} else if (line.contains("BEGIN IF")) {
				
	    		String[] terms = line.split(" ");
	    		
	    		boolean test = false;
	    		String name = null;
	    		
	    		if (terms[2].equals("NOT")) {
	    			name = terms[3];
	    			test = !_specificParams.containsKey(terms[3]);
	    		} else {
	    			name = terms[2];
	    			test = _specificParams.containsKey(terms[2]);
	    		}
	    			    		
	    		line = buff.readLine();
	    		
	    		if (test) {
	    			    			
	        		while (!line.contains("END IF " + name)) {
	        				        			
	        			/*line = replace(line);
	        			
	        			if (line.contains("=")) {
	        			
	        				String[] result = evaluate(line);
	        				
	        				parsedScript = parsedScript + result[0] + " = " + result[1] + "\n";
	        				
	        			} else {
	        				
	        				parsedScript = parsedScript + line + "\n";
	        				
	        			}
	        			
	        			line = buff.readLine();*/
	        			
	        			parsedScript = process(buff, line, parsedScript);
	        			
	        			line = buff.readLine();
	        			
	        		}
	    		
	    		} else {
	    				    			
	    			while (!line.contains("END IF " + name)) {		        			
	        			line = buff.readLine();
	        		}
	    			
	    		}
	    		
	    	} else if (line.contains("BEGIN FOR CLASSES")) {
	    		
	    		line = buff.readLine();
	    		
	    		while (!line.contains("END FOR CLASSES")) {
	    			
	    			String classes = _specificParams.get("$CLASSES");
	    			
	    			String[] array = classes.split(",");
	    			
	    			for (int c=0; c<array.length; c++) {
	    				String statement = line.replace("$CLASS", array[c]);
	    				parsedScript = process(buff, statement, parsedScript);
	    			}
	    			
	    			line = buff.readLine();
	    			
	    		}
	    		
	    	} else if (line.contains("EXPORT")) {
	    				        				        		
	    		String[] terms = line.split(" ");
	    		
	    		terms[1] = replace(terms[1]);
	    			    		
	    		_export.put(URL.getFileNameWithoutExtension(terms[1]),terms[1]);
	    		
	        	//parsedScript = parsedScript + line + "\n";
	        
	    	} else if (line.contains("INCLUDE")) {
	    		
	    		String[] terms = line.split(" ");
	    			    		
	    		parsedScript = parsedScript + parse(terms[1], false);
	    	
	    	} else if (line.contains("MLOAD")) {
	    		
	    		String input_path = _specificParams.get("$INPUT_PATH");
	    		
	    		String[] inputs = input_path.split(",");
	    		
	    		for (int i=0; i<inputs.length; i++) {
	    			String aux1 = "load = LOAD '" + inputs[i] + "' USING org.apache.pig.builtin.JsonLoader('geometry:chararray, data:map[chararray], properties:map[bytearray]');";
		    		String[] result1 = evaluate(aux1);	        		
	        		parsedScript = parsedScript + result1[0] + " = " + result1[1] + "\n";
	    		}
	    		
	    		String aux2 = "union = UNION ";
	    		
	    		boolean first = true;
	    		
	    		for (int i=0; i<inputs.length; i++) {
	    			if (first) {
	    				aux2 = aux2 + "load_" + (i+1);
	    				first = false;
	    			} else
	    				aux2 = aux2 + ", load_" + (i+1);
	    		}
	    		
	    		aux2 = aux2 + ";";
	    		
	    		String[] result2 = evaluate(aux2);
        		
        		parsedScript = parsedScript + result2[0] + " = " + result2[1] + "\n";
	    		
	    	} else {
	    		
	    		if (!line.trim().isEmpty()) {
	    		
		    		line = replace(line);
		    		
		        	if (line.contains("=") && (!line.trim().startsWith("DEFINE"))) {
		        		
		        		String[] result = evaluate(line);
		        				        		
		        		parsedScript = parsedScript + result[0] + " = " + result[1] + "\n";
		        		
		        	} else {
		        				        		
		        		parsedScript = parsedScript + line + "\n";
		        	}
	        	
	    		}
	
	    	}
						
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("It was not possible to parse the Pig script.");
		}
		
		return parsedScript;
		
	}
	
	public String parse(String str, boolean notInclude) {
		
		String script;
		
		if (_set.getOperators().containsKey(str)) {
			script = (String)_set.getOperators().get(str).get("template");
		} else {
			script = str;
		}
		
		_export.clear();
		_relations.clear();
		_relationMap.clear();
		
		String parsedScript = new String();
		
		try {
		
			InputStream is = new ByteArrayInputStream(script.getBytes());			
			
			InputStreamReader inStream = new InputStreamReader(is);
	        BufferedReader buff = new BufferedReader(inStream);
	        
	        String line;
	        while ((line = buff.readLine()) != null) {
	        	
	        	parsedScript = process(buff, line, parsedScript);
	        	
	        }
			        
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("It was not possible to parse the Pig script.");
		}
	    
		if (notInclude) {
		
			if (_specificParams.containsKey("$STORE")) {
				
				String relation = null;
				
				if (_specificParams.containsKey("$OUTPUT.ROI")) {
					relation = _globalRelations.get(_globalRelations.size()-2);
				} else {
					relation = _globalRelations.get(_globalRelations.size()-1);
				}
				
				parsedScript = parsedScript + "STORE " + relation + " INTO '" + _specificParams.get("$OUTPUT_PATH") + "' USING br.puc_rio.ele.lvc.interimage.common.udf.JsonStorage();\n";
			}
			
		}
		
		return parsedScript;
		
	}
	
	public List<String> getRelations() {
		return _relations;
	}
	
	public Map<String, List<String>> getRelationMap() {
		return _relationMap;
	}
	
	public List<String> getGlobalRelations() {
		return _globalRelations;
	}
	
	public Map<String, List<String>> getGlobalRelationMap() {
		return _globalRelationMap;
	}
	
	public Map<String,String> getExport() {
		return _export;
	}

	public String getResult() {
		return _specificParams.get("$OUTPUT_PATH");
	}
	
	/*@SuppressWarnings("unchecked")
	public Map<String, String> getOperatorInputs(String operatorName) {
		return (Map<String, String>)_set.getOperators().get(operatorName).get("inputs");
	}*/
	
}