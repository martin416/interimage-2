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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
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
import br.puc_rio.ele.lvc.interimage.common.Tile;
import br.puc_rio.ele.lvc.interimage.common.UUID;
import br.puc_rio.ele.lvc.interimage.geometry.FilterGeometryCollection;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * A UDF that clips geometries in relation to a list of ROIs.<br>
 * For efficiency reasons, it should always be used after SpatialFilter.<br><br>
 * 
 * Some observations:<br>
 * 1 - The geometries that do not intersect any ROI will be filtered out<br>
 * 2 - The geometries that intersect more than one ROI will produce the respective number of tuples<br>
 * 3 - The output geometries will contain the iiuuid of their parent ROI in the parent field
 * 4 - Makes no sense to call this UDF after SpatialFilter when it is used with the 'containment' filter type
 * <br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geom, data, props);<br>
 * 		B = filter A by SpatialFilter(geom, props#'tile');<br>
 * 		C = foreach B generate flatten(SpatialClip(geom, data, props)) as (geom, data, props);
 * @author Rodrigo Ferreira
 *
 */
public class SpatialClip extends EvalFunc<DataBag> {
	
	private final GeometryParser _geometryParser = new GeometryParser();
	private STRtree _gridIndex = null;
	private STRtree _roiIndex = null;
	private List<String> _gridIds = null;
	
	private String _roiUrl = null;
	private String _gridUrl = null;
	
	private double _minArea;
	
	/**Constructor that takes the ROIs and the tiles grid URLs.*/
	public SpatialClip(String roiUrl, String gridUrl, String minArea) {
		_roiUrl = roiUrl;
		_gridUrl = gridUrl;
		_minArea = Double.parseDouble(minArea);
	}
	
	/**
     * Method invoked on every tuple during filter evaluation.
     * @param input tuple<br>
     * first column is assumed to have the geometry<br>
     * second column is assumed to have the data<br>
     * third column is assumed to have the properties
     * @exception java.io.IOException
     * @return a bag with tuples of clipped geometries, or a null bag in case of no intersection
     * 
     * TODO: Use distributed cache; check if an index for the ROIs is necessary; deal with data
     */
	@SuppressWarnings("unchecked")
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
            return null;
        
		//executes initialization
		if (_gridIndex == null) {
			_gridIndex = new STRtree();
			_roiIndex = new STRtree();
			_gridIds = new ArrayList<String>();
			
			//Creates an index for the grid
	        try {
	        	
	        	if (!_gridUrl.isEmpty()) {
	    	        
	        		URL url  = new URL(_gridUrl);	        		
	                URLConnection urlConn = url.openConnection();
	                urlConn.connect();
			        InputStream buff = new BufferedInputStream(urlConn.getInputStream());				    	    	        
			        ObjectInputStream in = new ObjectInputStream(buff);
	    			
	    		    List<Tile> tiles = (List<Tile>)in.readObject();
	    		    
	    		    in.close();
				    
				    for (Tile t : tiles) {
				    	Geometry geometry = new WKTReader().read(t.getGeometry());
    					_gridIndex.insert(geometry.getEnvelopeInternal(),t.getCode());
				    }
			        			        
	        	}
	        } catch (Exception e) {
	        	e.printStackTrace();
				throw new IOException("Caught exception reading grid file ", e);
			}
	        
	        //Creates index for the ROIs
	        //Also creates a list with the Ids of the tiles that intersect the ROIs
	        try {
	        	
	        	if (!_roiUrl.isEmpty()) {
	        			      
	        		URL url  = new URL(_roiUrl);	        		
	                URLConnection urlConn = url.openConnection();
	                urlConn.connect();
			        InputStream buff = new BufferedInputStream(urlConn.getInputStream());				    	    	        
			        ObjectInputStream in = new ObjectInputStream(buff);
	    			
	    		    List<br.puc_rio.ele.lvc.interimage.common.Shape> shapes = (List<br.puc_rio.ele.lvc.interimage.common.Shape>)in.readObject();
	    		    
	    		    in.close();
				    
				    for (br.puc_rio.ele.lvc.interimage.common.Shape t : shapes) {				    	
				    	Geometry geometry = new WKTReader().read(t.getGeometry());
				    	_roiIndex.insert(geometry.getEnvelopeInternal(),t);
			        	_gridIds.addAll(_gridIndex.query(geometry.getEnvelopeInternal()));
				    }
	        		
	        		/*if (_roiUrl.endsWith(".wkts")) {
	        			
	        			URL url  = new URL(_roiUrl);	        		
		                URLConnection urlConn = url.openConnection();
		                urlConn.connect();
		                InputStream inStream = urlConn.getInputStream();
	        			
		                //File temp = File.createTempFile(br.puc_rio.ele.lvc.interimage.common.URL.getFileNameWithoutExtension(_roiUrl), ".wkt");
		                
		                //temp.deleteOnExit();
		                
		                byte[] compressed = ByteStreams.toByteArray(inStream);
		                
		                byte[] decompressed = ByteStreams.toByteArray(new SnappyInputStream(new ByteArrayInputStream(compressed)));
		                
		                Geometry geometry = new WKTReader().read(new String(decompressed));
		                
			        	_roiIndex.insert(geometry.getEnvelopeInternal(),geometry);
			        	_gridIds.addAll(_gridIndex.query(geometry.getEnvelopeInternal()));
	        			
	        		} else {	        		
	        		
		        		URL url  = new URL(_roiUrl);	        		
		                URLConnection urlConn = url.openConnection();
		                urlConn.connect();
		                InputStreamReader inStream = new InputStreamReader(urlConn.getInputStream());
				        BufferedReader buff = new BufferedReader(inStream);
				        
				        String line;
				        while ((line = buff.readLine()) != null) {
				        	Geometry geometry = new WKTReader().read(line);
				        	_roiIndex.insert(geometry.getEnvelopeInternal(),geometry);
				        	_gridIds.addAll(_gridIndex.query(geometry.getEnvelopeInternal()));
				        }
				        
	        		}*/

	        	}
	        } catch (Exception e) {
	        	e.printStackTrace();
				throw new IOException("Caught exception reading ROI file ", e);
			}
	        
		}
		
		try {

			Object objGeometry = input.get(0);
			Map<String,String> data = (Map<String,String>)input.get(1);
			Map<String,Object> properties = DataType.toMap(input.get(2));
						
			String tileStr = DataType.toString(properties.get("tile"));
			
			//converting from string T0000 to long 0000
			//Long tileId = Long.parseLong(tileStr.substring(1));
			
			DataBag bag = BagFactory.getInstance().newDefaultBag();
			
	    	if ((!_roiUrl.isEmpty()) && (!_gridUrl.isEmpty())) {
		        if (_gridIds.contains(tileStr)) {
		        	Geometry geometry = _geometryParser.parseGeometry(objGeometry);
	
	        		List<br.puc_rio.ele.lvc.interimage.common.Shape> list = _roiIndex.query(geometry.getEnvelopeInternal());
	  	        		
		        	for (br.puc_rio.ele.lvc.interimage.common.Shape shape : list) {
		        		
		        		Geometry geom = new WKTReader().read(shape.getGeometry());
		        		
		        		if (geom.intersects(geometry)) {
		        			
		        			Geometry g = geom.intersection(geometry);
		        			
		        			if (g.getNumGeometries()>1) {// if it's a geometry collection
								g = FilterGeometryCollection.filter(g);	//keeping only polygons
								
								if (g.isEmpty())
									continue;							
								
							} else if (g.getNumGeometries()==1) {
								if (!(g instanceof Polygon))
									continue;										
							} else if (g.isEmpty()) {
								continue;
							}
		        			
		        			for (int k=0; k<g.getNumGeometries(); k++) {//separating polygons in different records
		        			
			        			//byte[] bytes = new WKBWriter().write(g.getGeometryN(k));
			        			
		        				Geometry aux_geom = g.getGeometryN(k);
		        				
		        				if (!(aux_geom instanceof Polygon))
		        					continue;
		        				
		        				if (aux_geom.getArea() < _minArea)
		        					continue;
		        				
			        			Tuple t = TupleFactory.getInstance().newTuple(3);
			        			t.set(0,new WKTWriter().write(aux_geom));
			        			t.set(1,new HashMap<String,String>(data));
			        			
			        			HashMap<String,Object> props = new HashMap<String,Object>(properties);
			        			props.put("iiuuid",new UUID(null).random());
			        			props.put("parent", shape.getCode());
			        			t.set(2,props);
			        			
			        			bag.add(t);
			        			
		        			}
		        					        			
		        		}
		        		
		        	}
		        			        				        	
		        }
		        
	    	} else {	    		
	    		bag.add(input);	    		
	    	}
			
	    	return bag;
	    	
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
