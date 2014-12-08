package br.puc_rio.ele.lvc.interimage.common.udf;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.Common;
import br.puc_rio.ele.lvc.interimage.common.GeometryParser;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A UDF that tests whether a record is valid.<br><br>
 * Example:<br>
 * 		A = load 'mydata' as (geom, props);<br>
 * 		B = filter A by IsValid(geom, props, 'comma separated field names');<br>
 * @author Rodrigo Ferreira
 *
 */
public class IsValid extends EvalFunc<Boolean> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	
	/**
     * Method invoked on every tuple during filter evaluation.
     * @param input tuple<br>
     * first column is assumed to have a geometry<br>
     * second column is assumed to have the properties<br>
     * third column is assumed to have the property names to be validated
     * @exception java.io.IOException
     * @return boolean value
     */
	@Override
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
        
		try {			
			Object objGeometry = input.get(0);
			Map<String,Object> objProperties = DataType.toMap(input.get(1));
			String fields = DataType.toString(input.get(2));
			
			if (objGeometry != null) {
				
				Geometry geometry = _geometryParser.parseGeometry(objGeometry);			
				
				if (!geometry.isValid())
					return false;
				
				if (!geometry.isEmpty())
					return false;
				
			}
			
			if (!fields.isEmpty()) {
			
				List<String> flist = Arrays.asList(fields.split(","));
				
				for (int i=0; i<flist.size(); i++) {
					
					String value = DataType.toString(objProperties.get(flist.get(i)));
						
					if (value == null)
						return false;
						
					if (Common.isNumeric(value)) {
					
						Double num = Double.parseDouble(value);
						
						if ((num.isNaN()) || (num.isInfinite())) {
							return false;
						}
						
					}
					
				}
				
			}
			
			return true;
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }
	
}
