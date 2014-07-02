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
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that computes the relative area of objects of a specific class that exist in the neighborhood of another object.<br>
 * The neighborhood is defined by the distance parameter and the distances are calculated as the minimum distance between the objects.<br>
 * The distance should be an empty string to consider only connected neighbors.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom);<br>
 * 		B = load 'mydata2' as (geom);<br>
 * 		C = SpatialGroup(A,B,2);<br>
 * 		D = filter C by RelativeAreaOf(A::geom, A::group,'','classname');<br>
 * @author Rodrigo Ferreira
 */
public class RelativeAreaOf extends EvalFunc<Double> {
		
	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a geometry<br>
     * second column is assumed to have a bag<br>
     * third column is assumed to have a distance; empty string for connected neighbors<br>
     * fourth column is assumed to have a class name
     * @exception java.io.IOException
     * @return objects relative area
     */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Double exec(Tuple input) throws IOException {
				
		if (input == null || input.size() < 4)
            return null;
		
		try {
						
			Geometry geometry = _geometryParser.parseGeometry(input.get(0));
			DataBag bag = DataType.toBag(input.get(1));
			String distanceStr = DataType.toString(input.get(2));
			String className = DataType.toString(input.get(3));
			
			if (bag.size() == 0)
				return null;
			
			double totalArea = 0.0;
			double area = 0.0;
			
			Iterator it = bag.iterator();
	        while (it.hasNext()) {
	        	Tuple t = (Tuple)it.next();
	        	
	        	Geometry geom = _geometryParser.parseGeometry(t.get(0));
	        	
	        	boolean bool = false;
	        	
				if (distanceStr.isEmpty()) {	//connected neighbors
	        		if (geometry.intersects(geom)) {
	        			bool = true;
	        		}
	        	} else {	//within distance neighbors
	        		Double distance = Double.parseDouble(distanceStr);	        		
	        		if (geometry.isWithinDistance(geom, distance)) {
	        			bool = true;
	        		}
	        	}
	        	
				if (bool) {
	        		double a = geom.getArea();
		        	
	        		Map<String,Object> properties = (Map<String,Object>)t.get(2);
	        		
	        		String name = (String)properties.get("class");        		
	        		
	        		if (name.equals(className)) {		        		
	        			area += a;
		        	}
	        		
		        	totalArea += a;
				}
	        	
	        }
			
			return area / totalArea;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));		
    }
	
}
