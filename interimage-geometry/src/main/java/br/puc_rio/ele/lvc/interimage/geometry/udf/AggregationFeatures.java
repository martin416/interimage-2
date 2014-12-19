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

package br.puc_rio.ele.lvc.interimage.geometry.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.geometry.SmallestSurroundingRectangle;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A class that computes aggregation features for all the input polygons.<br>
 * (So far) The aggregated features cannot be spectral features.<br>
 * This class is not meant to be used globally. It should be used to aggregate features within a tile or within a parent object.
 * @author Rodrigo Ferreira
 */

public class AggregationFeatures extends EvalFunc<DataBag> {

	private final GeometryParser _geometryParser = new GeometryParser();
	String _features;
	private Map<String, Map<String, Object>> _featureMap;	//attribute, operation, params
	
	/**Constructor that takes the feature list.*/
	public AggregationFeatures(String features) {
		_features = features;		
	}
	
	private double computeFeature(String attribute, Geometry geom) {
		
		//TODO: implement more attributes
		
		if (attribute.equals("area")) {
			return geom.getArea();
		} else if (attribute.equals("rectangle_fit")) {
			Geometry ssRect = SmallestSurroundingRectangle.get(geom);			
			return geom.getArea() / ssRect.getArea();
		}
		return Double.NaN;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, Double> computeFeatures(DataBag bag) {
		
		Map<String, Double> results = new HashMap<String, Double>();
		
		try {
						
			Map<String, Integer> mean_count = new HashMap<String, Integer>();
			
			//initialize
			
			for (Map.Entry<String, Map<String, Object>> entry : _featureMap.entrySet()) {
				
				String feature = entry.getKey();
				String operation = (String)entry.getValue().get("operation");				
								
				if (operation.equals("sum")) {
					results.put(feature, 0.0);
				} else if (operation.equals("count")) {
					results.put(feature, 0.0);
				}
				
			}
			
			Iterator it = bag.iterator();
			
			while (it.hasNext()) {
				Tuple t = (Tuple)it.next();
				
				Object objGeometry = t.get(0);
				//Map<String,String> data = (Map<String,String>)t.get(1);
				Map<String,Object> properties = DataType.toMap(t.get(2));
								
				Geometry geometry = _geometryParser.parseGeometry(objGeometry);
				
				String className = DataType.toString(properties.get("class"));
				
				for (Map.Entry<String, Map<String, Object>> entry : _featureMap.entrySet()) {
				
					String feature = entry.getKey();
					String operation = (String)entry.getValue().get("operation");
					List<String> paramList = (List<String>)entry.getValue().get("params");
															
					if (operation.equals("max")) {
						
						String attrib = paramList.get(0);
						
						if (paramList.get(1).equals(className)) {
							
							if (results.containsKey(feature)) {
								double aux = results.get(feature);
								results.put(feature, Math.max(aux, computeFeature(attrib, geometry)));
							} else {
								results.put(feature, computeFeature(attrib, geometry));
							}
							
						}
						
					} else if (operation.equals("sum")) {
						
						String attrib = paramList.get(0);
						
						if (paramList.get(1).equals(className)) {
							
							if (results.containsKey(feature)) {
								double aux = results.get(feature);
								results.put(feature, aux + computeFeature(attrib, geometry));
							} else {
								results.put(feature, computeFeature(attrib, geometry));
							}
							
						}
						
					} else if (operation.equals("min")) {
						
						String attrib = paramList.get(0);
						
						if (paramList.get(1).equals(className)) {
							
							if (results.containsKey(feature)) {
								double aux = results.get(feature);
								results.put(feature, Math.min(aux, computeFeature(attrib, geometry)));
							} else {
								results.put(feature, computeFeature(attrib, geometry));
							}
							
						}
						
					} else if (operation.equals("count")) {
						
						if (paramList.get(0).equals(className)) {
							
							if (results.containsKey(feature)) {
								double aux = results.get(feature);
								results.put(feature, aux + 1.0);
							} else {
								results.put(feature, 1.0);
							}
							
						}
						
					} else if (operation.equals("mean")) {
						
						String attrib = paramList.get(0);
						
						if (paramList.get(1).equals(className)) {
							
							if (results.containsKey(feature)) {
								double aux = results.get(feature);
								results.put(feature, aux + computeFeature(attrib, geometry));
							} else {
								results.put(feature, computeFeature(attrib, geometry));
							}
							
							if (mean_count.containsKey(feature)) {
								int aux = mean_count.get(feature);
								mean_count.put(feature, aux+1);
							} else {
								mean_count.put(feature, 1);
							}
							
						}
						
					} else if (operation.equals("std")) {
						//TODO: implement this
					}
					
				}
				
			}
				
			for (Map.Entry<String, Integer> entry : mean_count.entrySet()) {
				String feature = entry.getKey();
				int count = entry.getValue();
				
				double value = results.get(feature);
				results.put(feature, value / (double)count);				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("It was not possible to compute aggregation features.");			
		}
		
		return results;
		
	}
	
	private void parseFeatures() {
		
		_featureMap = new HashMap<String, Map<String, Object>>();
		
		String[] expressions = _features.split(";");
		
		for (int i=0; i<expressions.length; i++) {
			
			int idx = expressions[i].indexOf("=");
			
			String term1 = expressions[i].substring(0,idx).trim();
			
			String term2 = expressions[i].substring(idx+1).trim();
			
			int idx1 = term2.indexOf("(");
			
			String name = term2.substring(0,idx1).trim();
			
			String list = term2.substring(idx1+1,term2.length()-1).trim();
			
			String[] params = list.split(",");
						
			List<String> paramList = new ArrayList<String>();
			
			for (int j=0; j<params.length; j++) {	
				paramList.add(params[j]);				
			}
			
			Map<String, Object> map = new HashMap<String, Object>();
			
			map.put("operation", name);
			map.put("params", paramList);
			
			_featureMap.put(term1, map);
						
		}
		
	}
	
	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a bag with the single parent object
     * second column is assumed to have a bag with the corresponding child objects
     * @exception java.io.IOException
     * @return a bag with the computed features in the properties
     */
	@SuppressWarnings("rawtypes")
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() < 2)
            return null;
			
		try {
			
			//_newBag = true;
			
			DataBag bag1 = DataType.toBag(input.get(0));
			DataBag bag2 = DataType.toBag(input.get(1));			
			
			Iterator it1 = bag1.iterator();
	        Tuple t1 = (Tuple)it1.next();
			
			//Object objGeom1 = t1.get(0);			
			Map<String,Object> props1 = DataType.toMap(t1.get(2));
			
			DataBag output = BagFactory.getInstance().newDefaultBag();
				
			if (_featureMap == null) {
				parseFeatures();
			}
			
			Map<String, Double> features = null;
					
			features = computeFeatures(bag2);
			
			for (Map.Entry<String, Double> entry : features.entrySet()) {
				props1.put(entry.getKey(), entry.getValue());
			}
					
	        output.add(t1);
	        
	        return output;
	        
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Caught exception processing input row ", e);
		}
	}
		
	@Override
    public Schema outputSchema(Schema input) {
		
		try {

			List<Schema.FieldSchema> list = new ArrayList<Schema.FieldSchema>();
			list.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
			list.add(new Schema.FieldSchema(null, DataType.MAP));
			list.add(new Schema.FieldSchema(null, DataType.MAP));
					
			Schema tupleSchema = new Schema(list);
			
			Schema.FieldSchema ts = new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE);
			
			Schema bagSchema = new Schema(ts);
			
			Schema.FieldSchema bs = new Schema.FieldSchema(null, bagSchema, DataType.BAG);
			
			return new Schema(bs);

		} catch (Exception e) {
			return null;
		}
		
    }
	
}
