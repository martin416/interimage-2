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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that returns the density of a geometry.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geom);<br>
 * 		B = foreach A generate Density(geom);<br>
 * @author Rodrigo Ferreira
 *
 */
public class Density extends EvalFunc<Double> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every tuple during foreach evaluation.
     * @param input tuple<br>
     * first column is assumed to have a geometry
     * @exception java.io.IOException
     * @return density of the geometry
     */
	@Override
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
            return null;
        
		try {			
			Object objGeometry = input.get(0);
			Geometry geometry = _geometryParser.parseGeometry(objGeometry);
			double area = geometry.getArea();
			Coordinate centroid = geometry.getCentroid().getCoordinate();
			
			Coordinate[] vertices = geometry.getCoordinates();
			
			double radius = -Double.MAX_VALUE;
			
			for (int v=0; v<vertices.length; v++) {
				double distance = vertices[v].distance(centroid);
				if (distance > radius)
					radius = distance;
			}

			return area / radius;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
	
}
