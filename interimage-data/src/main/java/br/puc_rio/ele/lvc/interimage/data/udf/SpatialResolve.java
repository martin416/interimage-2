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

package br.puc_rio.ele.lvc.interimage.data.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.numeric.integer.LongType;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;
import com.vividsolutions.jts.io.WKTWriter;

import br.puc_rio.ele.lvc.interimage.common.GeometryParser;
import br.puc_rio.ele.lvc.interimage.common.OrderedList;
import br.puc_rio.ele.lvc.interimage.common.UUID;
import br.puc_rio.ele.lvc.interimage.data.Image;

/**
 * A UDF that resolves spatial overlaps based on membership values.<br><br>
 * Example:<br>
 * 		A = load 'mydata1' as (geometry, data, properties);<br>
 * 		B = load 'mydata2' as (geometry, data, properties);<br>
 * 		C = cogroup A by properties#'tile', B by properties#'tile';<br>
 * 		D = flatten(SpatialResolve(A,B));
 * @author Rodrigo Ferreira
 * 
 */
public class SpatialResolve extends EvalFunc<DataBag> {
		
	private final GeometryParser _geometryParser = new GeometryParser();
	
	private double _minArea;
	private String _imageUrl;
	private String _image;
	
	public SpatialResolve(String minArea, String imageUrl, String image) {
		_minArea = Double.parseDouble(minArea);
		_imageUrl = imageUrl;
		_image = image;
	}
	
	@SuppressWarnings("unchecked")
	private void computeSpatialResolve(OrderedList list, DataBag output) throws Exception {
		
		Img<LongType> img = null;
		
		long id = 1;
		double resX = 0.0;
		double resY = 0.0;
		double[] tileGeoBox = new double[4];
        int width = 0;
        int height = 0;
        String tileStr = null;
        String crs = null;
        
        Map<Long, List<Object>> map1 = new HashMap<Long, List<Object>>();
        Map<String,String> data = null;
        
		while (!list.isEmpty()) {
			
			Tuple t = list.poll();
			
			Geometry geom = _geometryParser.parseGeometry(t.get(0));
			
			//System.out.println("Geom: " + geom);
			
			PreparedGeometry prep = PreparedGeometryFactory.prepare(geom);
			
			//creates a map with the object's properties
			
			Map<String,Object> properties = DataType.toMap(t.get(2));
			
			List<Object> lp = new ArrayList<Object>();
			
			lp.add(properties.get("classification"));
			lp.add(properties.get("class"));
			lp.add(properties.get("membership"));
			lp.add(properties.get("parent"));
			
			map1.put(id, lp);
			
			//Computes image info just once
			
			if (img == null) {
							
				data = (Map<String,String>)t.get(1);
				
				tileStr = DataType.toString(properties.get("tile"));
				crs = DataType.toString(properties.get("crs"));
				
				/*Getting width and height*/
	        	URL worldFile1 = new URL(_imageUrl + _image + "/" + tileStr + ".meta");
				URLConnection urlConn1 = worldFile1.openConnection();
	            urlConn1.connect();
				InputStreamReader inStream1 = new InputStreamReader(urlConn1.getInputStream());
		        BufferedReader reader1 = new BufferedReader(inStream1);
		        		        
		        String line1;
		        int index1 = 0;
		        while ((line1 = reader1.readLine()) != null) {
		        	if (!line1.trim().isEmpty()) {
		        		if (index1==1)
		        			width = Integer.parseInt(line1);
		        		else if (index1==2)
		        			height = Integer.parseInt(line1);
		        		else if (index1==3)
		        			tileGeoBox[0] = Double.parseDouble(line1);
		        		else if (index1==4)
		        			tileGeoBox[1] = Double.parseDouble(line1);
		        		else if (index1==5)
		        			tileGeoBox[2] = Double.parseDouble(line1);
		        		else if (index1==6)
		        			tileGeoBox[3] = Double.parseDouble(line1);
			        	index1++;
		        	}
		        }
				
		        resX = (tileGeoBox[2]-tileGeoBox[0])/width;
				resY = (tileGeoBox[1]-tileGeoBox[3])/height;		        
		        
				/*System.out.println("width: " + width);
				System.out.println("height: " + height);
				
				System.out.println("geo0: " + tileGeoBox[0]);
				System.out.println("geo1: " + tileGeoBox[1]);
				System.out.println("geo2: " + tileGeoBox[2]);
				System.out.println("geo3: " + tileGeoBox[3]);
				
				System.out.println("resX: " + resX);
				System.out.println("resY: " + resY);*/
				
		        img = new ArrayImgFactory<LongType>().create(new long[] {width, height}, new LongType());
		        
			}
		
			int[] bBox = Image.imgBBox(new double[] {geom.getEnvelopeInternal().getMinX(), geom.getEnvelopeInternal().getMinY(), geom.getEnvelopeInternal().getMaxX(), geom.getEnvelopeInternal().getMaxY()}, tileGeoBox, new int[] {width, height});
	        
	        double[] geoBBox = Image.geoBBox(bBox, tileGeoBox, new int[] {width, height});
				        
			//int gwidth = bBox[2]-bBox[0]+1;
			//int gheight = bBox[1]-bBox[3]+1;
						
			RandomAccess< LongType > r = img.randomAccess();
			
			/*System.out.println("bbox[0]: " + bBox[0]);
			System.out.println("bbox[1]: " + bBox[1]);
			System.out.println("bbox[2]: " + bBox[2]);
			System.out.println("bbox[3]: " + bBox[3]);
			
			System.out.println("geobbox[0]: " + geoBBox[0]);
			System.out.println("geobbox[1]: " + geoBBox[1]);
			System.out.println("geobbox[2]: " + geoBBox[2]);
			System.out.println("geobbox[3]: " + geoBBox[3]);*/
			
			//Rasterization
			for (int j=bBox[3]; j<=bBox[1]; j++) {
				for (int i=bBox[0]; i<=bBox[2]; i++) {
					
					double centerPixelX = geoBBox[0] + ((i-bBox[0])*resX) + (resX/2);
					double centerPixelY = geoBBox[3] + ((j-bBox[3])*resY) + (resY/2);
					
					Point point = new GeometryFactory().createPoint(new Coordinate(centerPixelX, centerPixelY));
					
					if (prep.covers(point)) {
						
						//System.out.println("covers: " + i + "," + j);
						
						r.setPosition(i, 0);
						r.setPosition(j, 1);
						LongType value = r.get();
						value.set(id);
						
						//System.out.println("put id: " + id);
						
						//System.out.println("img id: " + r.get().get());
					}
					
				}
			}
			
			id++;
			
		}
		
		//vectorization
				
		GeometryFactory fact = new GeometryFactory();
		
		Map<Long, List<Geometry>> map2 = new HashMap<Long, List<Geometry>>();
				
		Cursor<LongType> cursor = img.cursor();
		
		int[] pos = new int[2];
		
		while (cursor.hasNext()) {
			LongType value = cursor.next();
			long id2 = value.get();
			
			//invalid id
			
			if (!map1.containsKey(id2))
				continue;
			
			cursor.localize(pos);
			
			int x = pos[0];
			int y = pos[1];
			
			double CoordX, CoordY;
				
			Coordinate [] linePoints = new Coordinate[5];
			//left top corner
			CoordX = Image.imgToGeoX(x - 0.5, width, tileGeoBox);
			CoordY = Image.imgToGeoY(y - 0.5, height, tileGeoBox);
			linePoints[0]= new Coordinate(CoordX,CoordY);
			linePoints[4]= new Coordinate(CoordX,CoordY); //close the ring
            //right top corner
			CoordX = Image.imgToGeoX(x + 0.5, width, tileGeoBox);
			CoordY = Image.imgToGeoY(y - 0.5, height, tileGeoBox);
			linePoints[1] = new Coordinate(CoordX,CoordY);
			//right bottom corner
			CoordX = Image.imgToGeoX(x + 0.5, width, tileGeoBox);
			CoordY = Image.imgToGeoY(y + 0.5, height, tileGeoBox);
			linePoints[2] = new Coordinate(CoordX,CoordY);
			//right bottom corner
			CoordX = Image.imgToGeoX(x - 0.5, width, tileGeoBox);
			CoordY = Image.imgToGeoY(y + 0.5, height, tileGeoBox);
			linePoints[3] = new Coordinate(CoordX,CoordY);
			 
			LinearRing shell = fact.createLinearRing(linePoints);          			
  			Geometry poly = fact.createPolygon(shell, null);
		
  			if (map2.containsKey(id2)) {
  				List<Geometry> l = map2.get(id2);
  				l.add(poly);
  			} else {
  				List<Geometry> l = new ArrayList<Geometry>();
  				l.add(poly);
  				map2.put(id2, l);
  			}
  			  			
		}
				
		for (Map.Entry<Long, List<Geometry>> entry : map2.entrySet()) {
        	
			long lid = entry.getKey();
			
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
        		
        		if (aux.getArea() < _minArea)
        			continue;
        		
	        	Tuple t = TupleFactory.getInstance().newTuple(3);
					
        		//byte[] bytes = new WKBWriter().write(aux);
        		
        		Map<String,Object> props = new HashMap<String,Object>();
        		
        		String id2 = new UUID(null).random();
        		
        		//TODO: should we maintain the tile info here?
        		
        		props.put("crs", crs);        		
        		props.put("class", map1.get(lid).get(1));
        		props.put("tile", tileStr);
        		props.put("membership", "0.0");
        		props.put("iiuuid", id2);
        		props.put("parent", map1.get(lid).get(3));
        		        		
        		t.set(0,new WKTWriter().write(aux));
        		t.set(1,new HashMap<String,String>(data));
        		t.set(2,props);
        		output.add(t);
        		
        	}
    		
        }
		
	}
	
	/**
     * Method invoked on every bag during foreach evaluation.
     * @param input tuple<br>
     * the columns are assumed to have the bags
     * @exception java.io.IOException
     * @return a bag with the input bags spatially resolved
     */
	@SuppressWarnings("rawtypes")
	@Override
	public DataBag exec(Tuple input) throws IOException {
				
		if (input == null || input.size() == 0)
            return null;
		
		try {
						
			DataBag output = BagFactory.getInstance().newDefaultBag();
						
			OrderedList list = new OrderedList(input.size());
			
			for (int j=0; j<input.size(); j++) {
				DataBag bag = DataType.toBag(input.get(j));
				
				Iterator it = bag.iterator();
			    while (it.hasNext()) {
			        Tuple t = (Tuple)it.next();
			        Map<String,Object> props = DataType.toMap(t.get(2));					
					//String iiuuid = DataType.toString(props.get("iiuuid"));
			        
			        //Redefining iiuuid because some may be repeated
			        
			        String id = new UUID(null).random();
	        		props.put("iiuuid", id);
	        		t.set(2,props);
	        		
			        list.add(t);
			    }
			    
			}
					    
			computeSpatialResolve(list, output);
						
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
