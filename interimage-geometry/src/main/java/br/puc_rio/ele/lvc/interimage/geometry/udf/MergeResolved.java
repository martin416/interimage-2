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
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.UUID;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * A class that merges spatially resolved polygons.<br>
 * This class is not meant to be used alone. It should be used after SpatialResolve.
 * 
 * Contract:<br>
 * i) This class zeroes the 'membership' value of resolved objects;
 * ii) This class keeps the 'parent' value;
 * iii) This class removes the 'classification' info;
 * iv) This class removes the 'tile' info. CalculateTiles should be called afterwards if the tile id is needed;
 * v) Merged objects get new id's.
 * 
 * @author Rodrigo Ferreira
 */

public class MergeResolved extends EvalFunc<DataBag> {

	private final GeometryParser _geometryParser = new GeometryParser();
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void mergeResolved(DataBag bag, DataBag output) {
				
		try {
		
			Map<String, List<Geometry>> map1 = new HashMap<String, List<Geometry>>();
			Map<String, List<String>> map2 = new HashMap<String, List<String>>();
			
			String crs = null;
			Map<String,String> data = null;
						
			Iterator it = bag.iterator();
		    while (it.hasNext()) {
		        Tuple t = (Tuple)it.next();
		        
		        Geometry geometry = _geometryParser.parseGeometry(t.get(0));		        
		        Map<String,Object> props = DataType.toMap(t.get(2));				
				
		        String iiuuid = DataType.toString(props.get("iiuuid"));
		        String className = DataType.toString(props.get("class"));
		        String parent = DataType.toString(props.get("parent"));
		        
		        if (!map2.containsKey(iiuuid)) {
		        	List<String> l = new ArrayList<String>(2);
		        	l.add(className);
		        	l.add(parent);
		        	map2.put(iiuuid, l);
		        }
		        	
		        if (crs == null) {
		        	crs = DataType.toString(props.get("crs"));
		        	data = (Map<String,String>)t.get(1);		        			        	
		        }
		        
		        if (map1.containsKey(iiuuid)) {
		        	List<Geometry> l = map1.get(iiuuid);
		        	l.add(geometry);
		        } else {
		        	List<Geometry> l = new ArrayList<Geometry>();
		        	l.add(geometry);
		        	map1.put(iiuuid, l);
		        }
		        
		    }
		    		    
		    //merge
		    
		    GeometryFactory fact = new GeometryFactory();
		    
		    for (Map.Entry<String, List<Geometry>> entry : map1.entrySet()) {
	        	
				String iiuuid = entry.getKey();
				
	        	List<Geometry> list2 = entry.getValue();
	        	
	        	Geometry[] geoms = new Geometry[list2.size()];
	        	
	        	int index = 0;
	        	for (Geometry geom : list2) {
	        		geoms[index] = geom;
	        		index++;
	        	}
	        	
	        	//Geometry union = new GeometryCollection(geoms, new GeometryFactory()).buffer(0);
	        	Geometry union = fact.createGeometryCollection(geoms).buffer(0);
	        	
	        	for (int k=0; k<union.getNumGeometries(); k++) {
	        	
	        		Geometry aux = union.getGeometryN(k);
	        			        		
		        	Tuple t = TupleFactory.getInstance().newTuple(3);
						
	        		//byte[] bytes = new WKBWriter().write(aux);
	        		
	        		Map<String,Object> props = new HashMap<String,Object>();
	        		
	        		String id2 = new UUID(null).random();
	        		
	        		//TODO: how to handle parent and tile info?
	        		
	        		props.put("crs", crs);
	        		props.put("class", map2.get(iiuuid).get(0));
	        		props.put("tile", "");
	        		props.put("membership", "0.0");
	        		props.put("iiuuid", id2);
	        		props.put("parent", map2.get(iiuuid).get(1));
	        		        		
	        		t.set(0,new WKTWriter().write(aux));
	        		t.set(1,new HashMap<String,String>(data));
	        		t.set(2,props);
	        		output.add(t);
	        		
	        	}
	    		
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
	        
			mergeResolved(bag, output);
			
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
