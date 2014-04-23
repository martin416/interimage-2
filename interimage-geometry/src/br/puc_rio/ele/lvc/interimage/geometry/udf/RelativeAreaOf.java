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

import br.puc_rio.ele.lvc.interimage.geometry.GeometryParser;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that computes the relative area of objects of a specific class that exist in the neighborhood of another object.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom);<br>
 * 		B = load 'mydata2' as (geom);<br>
 * 		C = SpatialGroup(A,B,2);<br>
 * 		D = filter C by RelativeAreaOf(A::group,'classname');<br>
 * @author Rodrigo Ferreira
 */
public class RelativeAreaOf extends EvalFunc<Double> {
		
	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a bag<br>
     * second column is assumed to have a class name
     * @exception java.io.IOException
     * @return objects relative area
     */
	@SuppressWarnings("rawtypes")
	@Override
	public Double exec(Tuple input) throws IOException {
				
		if (input == null || input.size() < 2)
            return null;
		
		try {
						
			DataBag bag = DataType.toBag(input.get(0));
			String className = DataType.toString(input.get(1));
			
			double totalArea = 0.0;
			double area = 0.0;
			
			Iterator it = bag.iterator();
	        while (it.hasNext()) {
	        	Tuple t = (Tuple)it.next();
	        	Map<String,Object> properties = DataType.toMap(t.get(2));
	        	
	        	double a = 0.0;
	        	
	        	if (properties.get("area") != null) {
	        		a = (Double)properties.get("area");
	        	} else {
	        		Geometry geometry = _geometryParser.parseGeometry(t.get(0));
	        		a = geometry.getArea();
	        	}
	        	
        		if (((String)properties.get("class")).equals(className)) {		        		
        			area += a;
	        	}
        		
	        	totalArea += a;
	        	
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
