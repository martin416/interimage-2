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

/**
 * A UDF that resolves polygons that are spatially coincident.<br>
 * Actually, as polygons are coincident, there is no spatial resolve. The algorithm simply keeps the polygons with the highest membership value. 
 * @author Rodrigo Ferreira
 * */

public class SimpleSpatialResolve extends EvalFunc<DataBag> {

	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * the columns are assumed to have the bags
     * @exception java.io.IOException
     * @return a bag with the input bags spatially resolved
     */
	@SuppressWarnings({ "rawtypes" })
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() == 0)
            return null;
		
		try {
						
			DataBag output = BagFactory.getInstance().newDefaultBag();
						
			Map<String, List<Object>> map = new HashMap<String, List<Object>>();
			
			for (int j=0; j<input.size(); j++) {
				DataBag bag = DataType.toBag(input.get(j));
				
				Iterator it = bag.iterator();
			    while (it.hasNext()) {
			        Tuple tuple = (Tuple)it.next();
			        Map<String,Object> props = DataType.toMap(tuple.get(2));					
					
			        String iiuuid = DataType.toString(props.get("iiuuid"));
			        Double membership = DataType.toDouble(props.get("membership"));
			        String className = DataType.toString(props.get("class"));
			        
			        if (map.containsKey(iiuuid)) {
			        	List<Object> list = map.get(iiuuid);
			        	
			        	Double m = (Double)list.get(0);
			        	String cn = (String)list.get(1);
			        	//Tuple t = (Tuple)list.get(2);
			        	
			        	if (membership > m) {
			        		list.add(0,membership);
			        		list.add(1,className);
			        		list.add(2,tuple);
			        	} else if (membership == m) {
			        		
			        		if (className.compareTo(cn) > 0) {
			        			list.add(0,membership);
				        		list.add(1,className);
				        		list.add(2,tuple);
			        		}
			        		
			        	}
			        	
			        } else {
			        	List<Object> list = new ArrayList<Object>(2);
			        	list.add(membership);
			        	list.add(className);
			        	list.add(tuple);
			        	map.put(iiuuid, list);
			        }
			    }
			    
			}
							
			for (Map.Entry<String, List<Object>> entry : map.entrySet()) {
				List<Object> list = entry.getValue();				
				output.add((Tuple)list.get(1));				
			}
			
			return output;
			
		} catch (Exception e) {
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
