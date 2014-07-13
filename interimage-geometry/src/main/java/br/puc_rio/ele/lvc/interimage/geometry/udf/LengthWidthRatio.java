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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.geometry.SmallestSurroundingRectangle;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that returns the length width ratio of a geometry based on the smallest surrounding rectangle.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geom);<br>
 * 		B = foreach A generate LengthWidthRatio(geom);<br>
 * @author Rodrigo Ferreira
 *
 */
public class LengthWidthRatio extends EvalFunc<Double> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a geometry
     * @exception java.io.IOException
     * @return length width ratio of the geometry
     */
	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        
		try {			
			Object objGeometry = input.get(0);
			Geometry geometry = _geometryParser.parseGeometry(objGeometry);
			
			Geometry ssRect = SmallestSurroundingRectangle.get(geometry);
			
			Coordinate[] coords = ssRect.getCoordinates();
			double lg1 = coords[0].distance(coords[1]);
			double lg2 = coords[1].distance(coords[2]);
			
			if (lg1>lg2)
				return lg1/lg2;
			else
				return lg2/lg1;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}
