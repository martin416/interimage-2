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

package br.puc_rio.ele.lvc.interimage.common.udf;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import br.puc_rio.ele.lvc.interimage.common.Node;

/**
 * A UDF that selects geometries of a certain class, respecting the semantic net hierarchy.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom, data, props);<br>
 * 		B = filter A by SelectClass(props#'class', 'TargetClass');<br>
 * @author Rodrigo Ferreira
 *
 */
public class SelectClass extends EvalFunc<Boolean> {
	
	String _semanticNetUrl = null;
	Map<String,Node> _parents = null;
	//Map<String,List<Node>> _children = null;
		
	/**Constructor that takes the semantic network.*/
	public SelectClass(String semanticNetUrl) {
		_semanticNetUrl = semanticNetUrl;
	}
		
	private boolean isClass(String className, String targetClass) {
		if (className.equals(targetClass))
			return true;
		
		/*for (Node n : _children.get(className)) {
			return isClass(n.getClassName(), targetClass);
		}*/
		
		if (_parents.get(className) != null)
			return isClass(_parents.get(className).getClassName(), targetClass);
		else
			return false;
				
	}
	
	/**
     * Method invoked on every tuple during filter evaluation.
     * @param input tuple<br>
     * first column is assumed to have the object's class name<br>
     * second column is assumed to have the target class name<br>
     * @exception java.io.IOException
     * @return boolean value
     * 
     * TODO: Use distributed cache
     */
	@SuppressWarnings("unchecked")
	@Override
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2)
            return null;
        
		//executes initialization
		if (_parents == null) {
			
			//_parents = new HashMap<String,Node>();
			_parents = new HashMap<String,Node>();
			
			//Creates an index for the grid
	        try {
	        	
	        	if (!_semanticNetUrl.isEmpty()) {
	        		
	        		URL url  = new URL(_semanticNetUrl);	        		
	                URLConnection urlConn = url.openConnection();
	                urlConn.connect();
	                InputStream buff = new BufferedInputStream(urlConn.getInputStream());
	    	        ObjectInputStream in = new ObjectInputStream(buff);
	    			
	    		    List<Node> semanticNodes = (List<Node>)in.readObject();
	    		    
	    		    in.close();
				    
				    for (Node n : semanticNodes) {
				    	_parents.put(n.getClassName(), n.getParent());
				    					    	
				    	//_children.put(n.getClassName(), n.getChildren());
				    }
			        			        
	        	}
	        } catch (Exception e) {
	        	e.printStackTrace();
				throw new IOException("Caught exception reading semantic network file ", e);
			}
	       
	        
		}
		
		try {

			String className = DataType.toString(input.get(0));
			String targetClass = DataType.toString(input.get(1));
			
	    	if (!_semanticNetUrl.isEmpty()) {
	    		
	    		return isClass(className, targetClass);
		        				        	
	    	} else {
	    		return true;
	    	}
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }
	
}
