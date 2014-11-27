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
import java.util.Arrays;
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
import br.puc_rio.ele.lvc.interimage.common.SpatialIndex;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * A class that merges neighboring polygons of the same class.<br>
 * This class is not meant to be used globally. It should be used to merge objects within a tile or within a parent object.
 * @author Rodrigo Ferreira
 */

public class MergeNeighbors extends EvalFunc<DataBag> {

	private final GeometryParser _geometryParser = new GeometryParser();
	
	List<String> _mergeNeighborClasses;
	
	/**Constructor that takes "to be merged" classes.*/
	public MergeNeighbors(String mergeNeighborClasses) {
		_mergeNeighborClasses = Arrays.asList(mergeNeighborClasses.split(","));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void mergeNeighbors(DataBag bag) {
		
		//DataBag output = BagFactory.getInstance().newDefaultBag();
		
		try {
						
			Iterator it = bag.iterator();
		    while (it.hasNext()) {
		        Tuple t = (Tuple)it.next();
		        
		        SpatialIndex index = createIndex(bag);
		        
		        Geometry geom1 = _geometryParser.parseGeometry(t.get(0));
		        Map<String,Object> props1 = DataType.toMap(t.get(2));
		        
		        if (props1.containsKey("remove"))
		        	continue;
		        
		        String className1 = (String)props1.get("class");
		        String iiuuid1 = (String)props1.get("iiuuid");
		        
		        if (!_mergeNeighborClasses.contains(className1))
		        	continue;
		        
		        List<Tuple> neighbors = index.query(geom1.getEnvelopeInternal());
		        
		        for (Tuple t2 : neighbors) {
		        	
		        	Geometry geom2 = _geometryParser.parseGeometry(t2.get(0));
		        	Map<String,Object> props2 = DataType.toMap(t2.get(2));
		        	
		        	if (props2.containsKey("remove"))
		        		continue;
		        	
		        	String className2 = (String)props2.get("class");
		        	String iiuuid2 = (String)props2.get("iiuuid");
		        	
		        	if ((className2.equals(className1)) && (!iiuuid2.equals(iiuuid1))) {
		        		
		        		if (geom1.intersects(geom2)) {
		        			geom1 = geom1.union(geom2);
		        			props2.put("remove", "true");
		        			t2.set(2, props2);
		        		}
		        		
		        	}
		        	
		        }
		        
		        t.set(0, new WKTWriter().write(geom1));
		        
		    }
		    		    
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("It was not possible to merge neighboring objects.");	
		}
			
		//return output;
		
	}
	
	/**This method creates an STR-Tree index for the input bag and returns it.*/
	@SuppressWarnings("rawtypes")
	private SpatialIndex createIndex(DataBag bag) {
		
		SpatialIndex index = null;
		
		try {
		
			index = new SpatialIndex();
			
			Iterator it = bag.iterator();
	        while (it.hasNext()) {
	            Tuple t = (Tuple)it.next();
            	Geometry geometry = _geometryParser.parseGeometry(t.get(0));
            	Map<String,Object> props = DataType.toMap(t.get(2));
            	
            	if (props.containsKey("remove"))
            		continue;
            	
				index.insert(geometry.getEnvelopeInternal(),t);
	        }
		} catch (Exception e) {
			System.err.println("Failed to index bag; error - " + e.getMessage());
			return null;
		}
		
		return index;
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
				
		if (input == null || input.size() == 0)
            return null;
			
		try {
			
			DataBag bag = DataType.toBag(input.get(0));
			
			DataBag output = BagFactory.getInstance().newDefaultBag();
	        
			mergeNeighbors(bag);
			
			Iterator it = bag.iterator();
		    while (it.hasNext()) {
		        Tuple t = (Tuple)it.next();
		        
		        Map<String,Object> props = DataType.toMap(t.get(2));
		        
		        if (!props.containsKey("remove"))
		        	output.add(t);		        
		        
		    }
			
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
