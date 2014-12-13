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

package br.puc_rio.ele.lvc.interimage.data.udf;

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

/**
 * A class that combines partial values of spectral features.<br>
 * This class is not meant to be used alone. It should be used after SpectralFeatures.
 * 
 * @author Rodrigo Ferreira
 */

public class CombineSpectralFeatures extends EvalFunc<DataBag> {

	//private final GeometryParser _geometryParser = new GeometryParser();
	
	@SuppressWarnings({ "rawtypes" })
	private void combineFeatures(DataBag bag, DataBag output) {
				
		try {
			
			Map<String, List<Tuple>> tupleMap = new HashMap<String, List<Tuple>>();
			Map<String, Tuple> finalMap = new HashMap<String, Tuple>();
												
			Iterator it = bag.iterator();
		    while (it.hasNext()) {
		        Tuple t = (Tuple)it.next();
		        
		        //Geometry geometry = _geometryParser.parseGeometry(t.get(0));		        
		        Map<String,Object> props = DataType.toMap(t.get(2));				
				
		        String iiuuid = DataType.toString(props.get("iiuuid"));
		        		        
		        if (tupleMap.containsKey(iiuuid)) {
		        	List<Tuple> l = tupleMap.get(iiuuid);
		        	l.add(t);
		        } else {
		        	List<Tuple> l = new ArrayList<Tuple>();
		        	l.add(t);
		        	tupleMap.put(iiuuid, l);
		        }
		       		        		        
		        if (!finalMap.containsKey(iiuuid)) {
		        	if (!props.containsKey("iirep"))
		        		finalMap.put(iiuuid, t);
		        }
		      		        		        
		    }
		    		    
		    //compute features
		    
		    for (Map.Entry<String, List<Tuple>> entry1 : tupleMap.entrySet()) {
		    	
		    	String iiuuid = entry1.getKey();
		    	
		    	Map<String, Map<String, Object>> featureMap = new HashMap<String, Map<String, Object>>();
		    	
		    	List<Tuple> list = entry1.getValue();
		    	
		    	for (Tuple t : list) {
		    	
		    		Map<String,Object> props = DataType.toMap(t.get(2));
		    		
			    	Map<String, Object> spectral_features = DataType.toMap(props.get("spectral_features"));
			        
			        for (Map.Entry<String, Object> entry2 : spectral_features.entrySet()) {
			        	
			        	String name = entry2.getKey();
			        	
			        	Map<String, Object> params = DataType.toMap(entry2.getValue());
			        	
			        	String operation = DataType.toString(params.get("name"));
			        	
			        	//TODO: tile and image info could be used to combine GLCM texture features
			        	
			        	//String tile = (String)params.get("tile");
			        	//String image = (String)params.get("image");
			        	
			        	if (operation.equals("mean")) {
			        		
			        		Double sum = DataType.toDouble(params.get("sum"));
		        			Integer count = DataType.toInteger(params.get("count"));
			        		
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double s = (Double)m.get("sum");
			        			Integer c = (Integer)m.get("count");
			        			
			        			m.put("sum", s + sum);
			        			m.put("count", c + count);
			        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        						        			
			        			m.put("sum", sum);
			        			m.put("count", count);
			        			
			        			m.put("name", operation);
			        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("maxPixelValue")) {
			        		
			        		Double max = DataType.toDouble(params.get("max"));
			        		
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double x = (Double)m.get("max");
			        						        			
			        			m.put("max", Math.max(x, max));
			        						        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        			
			        			m.put("max", max);
			        			
			        			m.put("name", operation);
			        						        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("minPixelValue")) {
			        		
			        		Double min = DataType.toDouble(params.get("min"));
			        		
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double n = (Double)m.get("min");
			        						        			
			        			m.put("min", Math.min(n, min));
			        						        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        			
			        			m.put("min", min);
			        			
			        			m.put("name", operation);
			        						        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("ratio")) {
			        		
			        		Integer bands = DataType.toInteger(params.get("bands"));
			        		Integer band = DataType.toInteger(params.get("band"));
		        			Integer count = DataType.toInteger(params.get("count"));
			        		
		        			Double[] sum = new Double[bands];
		        			
		        			for (int i=0; i<bands; i++) {
		        				sum[i] = DataType.toDouble(params.get("band_" + i));
		        			}
		        			
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Integer b = (Integer)m.get("bands");
			        			Integer c = (Integer)m.get("count");
			        			
			        			Double[] s = new Double[bands];
			        			
			        			for (int i=0; i<b; i++) {
			        				s[i] = (Double)params.get("band_" + i);
			        				m.put("band_" + i, s[i] + sum[i]);
			        			}
			        			
			        			m.put("count", c + count);
			        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        						        			
			        			for (int i=0; i<bands; i++) {			        				
			        				m.put("band_" + i, sum[i]);
			        			}
			        			
			        			m.put("count", count);
			        			m.put("bands", bands);
			        			m.put("band", band);
			        			
			        			m.put("name", operation);
			        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("brightness")) {
			        					        		
			        		Integer bands = DataType.toInteger(params.get("bands"));
			        		
		        			Integer count = DataType.toInteger(params.get("count"));
			        		
		        			Double[] sum = new Double[bands];
		        			
		        			for (int i=0; i<bands; i++) {
		        				sum[i] = DataType.toDouble(params.get("band_" + i));
		        			}
		        			
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Integer b = (Integer)m.get("bands");
			        			Integer c = (Integer)m.get("count");
			        			
			        			Double[] s = new Double[bands];
			        			
			        			for (int i=0; i<b; i++) {
			        				s[i] = (Double)params.get("band_" + i);
			        				m.put("band_" + i, s[i] + sum[i]);
			        			}
			        			
			        			m.put("count", c + count);
			        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        						        			
			        			for (int i=0; i<bands; i++) {			        				
			        				m.put("band_" + i, sum[i]);
			        			}
			        			
			        			m.put("count", count);
			        			m.put("bands", bands);
			        			
			        			m.put("name", operation);
			        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("amplitudeValue")) {
			        		
			        		Double min = DataType.toDouble(params.get("min"));
			        		Double max = DataType.toDouble(params.get("max"));
			        		
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double n = (Double)m.get("min");
			        			Double x = (Double)m.get("max");
			        						        			
			        			m.put("min", Math.min(n, min));
			        			m.put("max", Math.max(x, max));
			        						        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        			
			        			m.put("min", min);
			        			m.put("max", max);
			        			
			        			m.put("name", operation);
			        						        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("standardDeviation")) {
			        	
			        		Double sum = DataType.toDouble(params.get("sum"));
			        		Double squared = DataType.toDouble(params.get("squared"));
			        		Integer count = DataType.toInteger(params.get("count"));
			        		
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double s = (Double)m.get("sum");
			        			Double q = (Double)m.get("squared");
			        			Integer c = (Integer)m.get("count");
			        						        			
			        			m.put("sum", s + sum);
			        			m.put("squared", q + squared);
			        			m.put("count", c + count);			        			
			        						        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        			
			        			m.put("sum", sum);
			        			m.put("squared", squared);
			        			m.put("count", count);
			        			
			        			m.put("name", operation);
			        						        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("bandMeanAdd")) {
			        		
			        		Double band_a = DataType.toDouble(params.get("band_a"));
			        		Double band_b = DataType.toDouble(params.get("band_b"));
			        		
		        			Integer count = DataType.toInteger(params.get("count"));
		        			
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double ba = (Double)m.get("band_a");
			        			Double bb = (Double)m.get("band_b");
			        			
			        			Integer c = (Integer)m.get("count");

			        			m.put("band_a", ba + band_a);
			        			m.put("band_b", bb + band_b);
			        			
			        			m.put("count", c + count);
			        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        						        			
			        			m.put("count", count);
			        			m.put("band_a", band_a);
			        			m.put("band_b", band_b);
			        			
			        			m.put("name", operation);
			        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("bandMeanDiv")) {
			        		
			        		Double band_a = DataType.toDouble(params.get("band_a"));
			        		Double band_b = DataType.toDouble(params.get("band_b"));
			        		
		        			Integer count = DataType.toInteger(params.get("count"));
		        			
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double ba = (Double)m.get("band_a");
			        			Double bb = (Double)m.get("band_b");
			        			
			        			Integer c = (Integer)m.get("count");

			        			m.put("band_a", ba + band_a);
			        			m.put("band_b", bb + band_b);
			        			
			        			m.put("count", c + count);
			        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        						        			
			        			m.put("count", count);
			        			m.put("band_a", band_a);
			        			m.put("band_b", band_b);
			        			
			        			m.put("name", operation);
			        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("bandMeanMul")) {
			        		
			        		Double band_a = DataType.toDouble(params.get("band_a"));
			        		Double band_b = DataType.toDouble(params.get("band_b"));
			        		
		        			Integer count = DataType.toInteger(params.get("count"));
		        			
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double ba = (Double)m.get("band_a");
			        			Double bb = (Double)m.get("band_b");
			        			
			        			Integer c = (Integer)m.get("count");

			        			m.put("band_a", ba + band_a);
			        			m.put("band_b", bb + band_b);
			        			
			        			m.put("count", c + count);
			        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        						        			
			        			m.put("count", count);
			        			m.put("band_a", band_a);
			        			m.put("band_b", band_b);
			        			
			        			m.put("name", operation);
			        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	} else if (operation.equals("bandMeanSub")) {
			        		
			        		Double band_a = DataType.toDouble(params.get("band_a"));
			        		Double band_b = DataType.toDouble(params.get("band_b"));
			        		
		        			Integer count = DataType.toInteger(params.get("count"));
		        			
			        		if (featureMap.containsKey(name)) {
			        			Map<String, Object> m = featureMap.get(name);
			        			
			        			Double ba = (Double)m.get("band_a");
			        			Double bb = (Double)m.get("band_b");
			        			
			        			Integer c = (Integer)m.get("count");

			        			m.put("band_a", ba + band_a);
			        			m.put("band_b", bb + band_b);
			        			
			        			m.put("count", c + count);
			        			
			        		} else {
			        			
			        			HashMap<String, Object> m = new HashMap<String, Object>();
			        						        			
			        			m.put("count", count);
			        			m.put("band_a", band_a);
			        			m.put("band_b", band_b);
			        			
			        			m.put("name", operation);
			        			
			        			featureMap.put(name, m);
			        			
			        		}
			        		
			        	}
			        	
			        }
			        
		    	}
			    
		    	//combine features
		    	
		    	Tuple t = finalMap.get(iiuuid);
		    	
		    	Map<String,Object> props = DataType.toMap(t.get(2));
		    	
		    	for (Map.Entry<String, Map<String, Object>> entry3 : featureMap.entrySet()) {
		    	
		    		String feature = entry3.getKey();
		    		Map<String, Object> m = entry3.getValue();
		    		
		    		String operation = (String)m.get("name");
		    		
		    		if (operation.equals("mean")) {
		    			
		    			Double sum = (Double)m.get("sum");
		    			Integer count = (Integer)m.get("count");
		    			
	    				props.put(feature, sum/count);
		    			
		    		} else if (operation.equals("maxPixelValue")) {
		    			
		    			Double max = (Double)m.get("max");
		    			
	    				props.put(feature, max);
		    			
		    		} else if (operation.equals("minPixelValue")) {
		    			
		    			Double min = (Double)m.get("min");
		    			
		    			props.put(feature, min);
		    					    			
		    		} else if (operation.equals("amplitudeValue")) {
		    			
		    			Double max = (Double)m.get("max");
		    			Double min = (Double)m.get("min");
		    			
		    			props.put(feature, max-min);
		    			
		    		} else if (operation.equals("standardDeviation")) {
		    			
		    			Double sum = (Double)m.get("sum");
		    			Double squared = (Double)m.get("squared");
		    			Integer count = (Integer)m.get("count");
		    			
		    			Double mean = sum/count;
		    			
		    			props.put(feature, Math.sqrt(Math.abs((squared - 2*mean*sum + count*mean*mean))/count));
		    			
		    			
		    		} else if (operation.equals("ratio")) {
		    			
		    			Integer bands = (Integer)m.get("bands");
		    			Integer band = (Integer)m.get("band");
		    			Integer count = (Integer)m.get("count");
		    			
		    			Double sum = 0.0;
		    			Double bandMean = 0.0;
		    			
		    			for (int i=0; i<bands; i++) {
	        				Double s = (Double)m.get("band_" + i);
	        				Double mean = s / count;
	        				
	        				if (i==band)
	        					bandMean = mean;
	        				
	        				sum = sum + mean;
	        			}
		    			
		    			props.put(feature, bandMean/sum);
		    			
		    		} else if (operation.equals("brightness")) {
		    			
		    			Integer bands = (Integer)m.get("bands");		    			
		    			Integer count = (Integer)m.get("count");
		    			
		    			Double sum = 0.0;
		    			
		    			for (int i=0; i<bands; i++) {
	        				Double s = (Double)m.get("band_" + i);
	        				Double mean = s / count;
	        				sum = sum + mean;
	        			}
		    			
		    			props.put(feature, sum/bands);
		    			
		    		} else if (operation.equals("bandMeanAdd")) {
		    			
		    			Double band_a = (Double)m.get("band_a");
		    			Double band_b = (Double)m.get("band_b");
		    			Integer count = (Integer)m.get("count");

		    			Double mean_a = band_a / count;
		    			Double mean_b = band_b / count;
		    					    			
		    			props.put(feature, mean_a + mean_b);
		    			
		    		} else if (operation.equals("bandMeanDiv")) {
		    			
		    			Double band_a = (Double)m.get("band_a");
		    			Double band_b = (Double)m.get("band_b");
		    			Integer count = (Integer)m.get("count");

		    			Double mean_a = band_a / count;
		    			Double mean_b = band_b / count;
		    					    			
		    			props.put(feature, mean_a / mean_b);
		    			
		    		} else if (operation.equals("bandMeanMul")) {
		    			
		    			Double band_a = (Double)m.get("band_a");
		    			Double band_b = (Double)m.get("band_b");
		    			Integer count = (Integer)m.get("count");

		    			Double mean_a = band_a / count;
		    			Double mean_b = band_b / count;
		    					    			
		    			props.put(feature, mean_a * mean_b);
		    			
		    		} else if (operation.equals("bandMeanSub")) {
		    			
		    			Double band_a = (Double)m.get("band_a");
		    			Double band_b = (Double)m.get("band_b");
		    			Integer count = (Integer)m.get("count");

		    			Double mean_a = band_a / count;
		    			Double mean_b = band_b / count;
		    					    			
		    			props.put(feature, mean_a - mean_b);
		    			
		    		}
		    				    	
		    	}
		    	
		    	t.set(2, props);
		    	output.add(t);		    	
		    			    	
			}
		    		    
		    
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("It was not possible to merge neighboring objects.");	
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
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() == 0)
            return null;
			
		try {
			
			DataBag bag = DataType.toBag(input.get(0));
			
			DataBag output = BagFactory.getInstance().newDefaultBag();
	        
			combineFeatures(bag, output);
			
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
