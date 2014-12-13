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

package br.puc_rio.ele.lvc.interimage.data;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.ops.operation.iterable.unary.Max;
import net.imglib2.ops.operation.iterable.unary.Min;
import net.imglib2.ops.operation.iterable.unary.Sum;
import net.imglib2.roi.BinaryMaskRegionOfInterest;
import net.imglib2.type.logic.BitType;
import net.imglib2.type.numeric.real.DoubleType;
import br.puc_rio.ele.lvc.interimage.common.Common;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

/**
 * A class that computes spectral features. 
 * @author Rodrigo Ferreira
 */
public class FeatureCalculator {
	
	//private final DataParser imageParser = new DataParser();
	
	@SuppressWarnings("unchecked")
	public Map<String, Map<String, Object>> computeFeatures(Map<String, Map<String, Map<String, Object>>> imageMap, Map<String, Map<String, Object>> featureMap, Geometry geometry) {
		
		Map<String, Map<String, Object>> result = new HashMap<String, Map<String, Object>>();
		
		Map<String, Map<String, Map<Integer, Object>>> tileMap = new HashMap<String, Map<String, Map<Integer, Object>>>();
				
		Map<String, Object> masks = new HashMap<String, Object>();
		
		Map<String,Integer> bandsMap = new HashMap<String, Integer>();
				
		//Going through the attributes
		for (Map.Entry<String, Map<String, Object>> entry : featureMap.entrySet()) {
			
			String attribute = entry.getKey();
			
			Map<String, Object> params = new HashMap<String, Object>();
			
			//System.out.println(attribute);
			
			Map<String, Object> map = entry.getValue();
			
			String operation = (String)map.get("operation");
			List<String> paramList = (List<String>)map.get("params");
			
			//System.out.println(attribute);
			//System.out.println(operation);
			
			//if (operation.equals("mean")) {
				
			for (String param : paramList) {
			
				//System.out.println(param);
				
				//get image key and band in the form: image_layer2 or image
				String[] tokens = param.split("_");
				
				String imageKey = null;
				
				if (tokens.length>0) {
					imageKey = tokens[0];
				} else if (!Common.isNumeric(tokens[0])) {
					imageKey = tokens[0];
				} else {
					continue;
				}
				
				//System.out.println(imageKey);
				
				if (!tileMap.containsKey(imageKey)) {//does this only one time for each image
				
					//System.out.println(imageKey);
					
					Map<String, Map<Integer, Object>> tiles = new HashMap<String, Map<Integer, Object>>();
					
					/*computing tiles*/				
					for (Map.Entry<String, Map<String, Object>> entry2 : imageMap.get(imageKey).entrySet()) {
						
						String tile = entry2.getKey();
						
						//System.out.println(tile);
						
						Map<String, Object> map2 = entry2.getValue();
						
						BufferedImage buff = (BufferedImage)map2.get("image");
						
						if (!bandsMap.containsKey(imageKey)) {
							bandsMap.put(imageKey, buff.getRaster().getNumBands());
						}
						
						double[] tileGeoBox = (double[])map2.get("geoBox");
										
						Geometry tileGeom = null;
						
						//System.out.println(tile);
						
						try {
						
							tileGeom = new GeometryFactory().createPolygon(new Coordinate[] { new Coordinate(tileGeoBox[0], tileGeoBox[1]), new Coordinate(tileGeoBox[2], tileGeoBox[1]), new Coordinate(tileGeoBox[2], tileGeoBox[3]), new Coordinate(tileGeoBox[0], tileGeoBox[3]), new Coordinate(tileGeoBox[0], tileGeoBox[1])});
							
							//tileGeom = new WKTReader().read(String.format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", tileGeoBox[0], tileGeoBox[1], tileGeoBox[2], tileGeoBox[1], tileGeoBox[2], tileGeoBox[3], tileGeoBox[0], tileGeoBox[3], tileGeoBox[0], tileGeoBox[1]));
						
						} catch (Exception e) {
							System.err.println("Couldn't create tile geometry.");
							e.printStackTrace();
						}
												
						if (!tileGeom.intersects(geometry)) //{	//if geometry intersects tile
							continue;
						
							Map<Integer,Object> map3 = new HashMap<Integer,Object>();
							
							//System.out.println("intersects");
							
							/*Gets the part of the polygon inside the tile*/
							//Geometry geom = tileGeom.intersection(geometry);
							Geometry geom = geometry;
							
							int[] bBox = Image.imgBBox(new double[] {geom.getEnvelopeInternal().getMinX(), geom.getEnvelopeInternal().getMinY(), geom.getEnvelopeInternal().getMaxX(), geom.getEnvelopeInternal().getMaxY()}, tileGeoBox, new int[] {buff.getWidth(), buff.getHeight()});
							
							//System.out.println(bBox[0]);
							//System.out.println(bBox[1]);
							//System.out.println(bBox[2]);
							//System.out.println(bBox[3]);
							
							if ((bBox[0] < 0) || (bBox[1] < 0) || (bBox[2] < 0) || (bBox[3] < 0)
							|| (bBox[0] >= buff.getWidth()) || (bBox[1] >= buff.getHeight()) || (bBox[2] >= buff.getWidth()) || (bBox[3] >= buff.getHeight()))
								continue;
														
							/*int imgidx = 0;
							
							FloatProcessor ip = new FloatProcessor(bBox[2]-bBox[0]+1, bBox[1]-bBox[3]+1);
							
							ImageProcessor mask = new ShortProcessor(bBox[2]-bBox[0]+1, bBox[1]-bBox[3]+1);
							
							for (int j=0; j<buff.getHeight(); j++) {
								for (int i=0; i<buff.getWidth(); i++) {
									float dArray[] = new float[buff.getRaster().getNumDataElements()];
									buff.getRaster().getPixel(i, j, dArray);
									
									//Create mask: magic!
									
									//Set processor
									ip.setf(imgidx%buff.getWidth(), imgidx/buff.getWidth(), dArray[band]);
									imgidx++;
								}
							}
							
							ip.setMask(mask);
							
							map3.put("imageProcessor",ip);*/
							
							double[] geoBBox = Image.geoBBox(bBox, tileGeoBox, new int[] {buff.getWidth(), buff.getHeight()});
							
							int width = bBox[2]-bBox[0]+1;
							int height = bBox[1]-bBox[3]+1;
							
							double resX = (tileGeoBox[2]-tileGeoBox[0])/buff.getWidth();
							double resY = (tileGeoBox[1]-tileGeoBox[3])/buff.getHeight();
							
							Img<BitType> mask = null;
							
							if (!masks.containsKey(tile)) {
							
								//System.out.println("Computing mask for tile: " + tile);
										
								//long startTime = System.nanoTime();
								
								/*Creates mask*/
								mask = new ArrayImgFactory<BitType>().create(new long[] {width, height} , new BitType());
								Cursor<BitType> c = mask.cursor();
								
								//int idx = 0;
								int count = 0;
								
								geom = geom.buffer(0);
								
								PreparedGeometry prep = PreparedGeometryFactory.prepare(geom);
								
								int[] pos = new int[2];
								
								while(c.hasNext()) {
									BitType t = c.next();
									//int x = idx%(width);
									//int y = idx/(width);
									
									c.localize(pos);
									
									int x = pos[0];
									int y = pos[1];
									
									double centerPixelX = geoBBox[0] + (x*resX) + (resX/2);
									double centerPixelY = geoBBox[3] + (y*resY) + (resY/2);
									
									Point point = new GeometryFactory().createPoint(new Coordinate(centerPixelX, centerPixelY));
									
									boolean b = false;
									/*for (int g=0; g<geom.getNumGeometries(); g++) {
										if (geom.getGeometryN(g).covers(point)) {
											b = true;
										}
									}*/
									
									if (prep.covers(point))
										b = true;
									
									if (b)
										count++;
									
									t.set(b);
									
									//idx++;
								}
								
								masks.put(tile, mask);
								
								map3.put(1000,count);	//TODO: using 1000 as a code for the area, assuming no image will have 1000 bands!
								
								//long endTime = System.nanoTime();
								
								//System.out.println("mask in nanoseconds: " + (endTime-startTime));
								
							} else {
								
								//System.out.println("loaded mask for tile: " + tile);
								
								mask = (Img<BitType>)masks.get(tile);
							}
														
							WritableRaster raster = buff.getRaster();
							
							int bands = raster.getNumBands();
							
							/*Creates image*/
							
							//System.out.println("bands: " + bands);
							
							Object[] imgs = new Object[bands];
							Object[] cursors = new Object[bands];
							
							//long startTime = System.nanoTime();
							
							for (int b=0; b<bands; b++) {
								Img<DoubleType> img = new ArrayImgFactory<DoubleType>().create(new long[] {width, height}, new DoubleType());
								imgs[b] = img;
								cursors[b] = img.randomAccess();
							}
									
							for (int b=0; b<bands; b++) {
								
								RandomAccess<DoubleType> ra = (RandomAccess<DoubleType>)cursors[b];
								
								for (int j=bBox[3]; j<=bBox[1]; j++) {
									for (int i=bBox[0]; i<=bBox[2]; i++) {
										
											double value = raster.getSampleDouble(i, j, b);
											
											ra.setPosition(i-bBox[0],0);
											ra.setPosition(j-bBox[3],1);
											
											DoubleType pixelValue = ra.get();
											pixelValue.set(value);
									}
								}
							}
														
							for (int b=0; b<bands; b++) {
								/*Creates masked cursor*/
								BinaryMaskRegionOfInterest<BitType, Img<BitType>> x = new BinaryMaskRegionOfInterest<BitType, Img<BitType>>(mask);
								
								Cursor<DoubleType> cursor2 = x.getIterableIntervalOverROI(((Img<DoubleType>)imgs[b])).cursor();
								
								map3.put(b, cursor2);
							}
							
							//long endTime = System.nanoTime();
							
							//System.out.println("copying in nanoseconds: " + (endTime-startTime));
							
							tiles.put(tile, map3);
							
						//}
									
					}
					
					tileMap.put(imageKey, tiles);
					
				}
							
			}
			
			//long startTime = System.nanoTime();
			
			params.put("name", operation);
			
			if (operation.equals("mean")) {
				String[] tokens = paramList.get(0).split("_");
				int band = Integer.parseInt(tokens[1].replace("layer",""))-1;				
				meanValue(tileMap.get(tokens[0]), band, params);
				//result.put(attribute, meanValue(tileMap.get(tokens[0]), band));
			} else if (operation.equals("maxPixelValue")) {
				String[] tokens = paramList.get(0).split("_");
				int band = Integer.parseInt(tokens[1].replace("layer",""))-1;				
				maxPixelValue(tileMap.get(tokens[0]), band, params);
				//result.put(attribute, maxPixelValue(tileMap.get(tokens[0]), band));
			} else if (operation.equals("minPixelValue")) {
				String[] tokens = paramList.get(0).split("_");
				int band = Integer.parseInt(tokens[1].replace("layer",""))-1;
				minPixelValue(tileMap.get(tokens[0]), band, params);
				//result.put(attribute, minPixelValue(tileMap.get(tokens[0]), band));
			} else if (operation.equals("ratio")) {
				String[] tokens = paramList.get(0).split("_");
				int band = Integer.parseInt(tokens[1].replace("layer",""))-1;
				ratioValue(tileMap.get(tokens[0]), band, bandsMap.get(tokens[0]), params);
				//result.put(attribute, ratioValue(tileMap.get(tokens[0]), band, bandsMap.get(tokens[0])));
			} else if (operation.equals("brightness")) {
				String token = paramList.get(0).trim();
				brightnessValue(tileMap.get(token),bandsMap.get(token), params);
				//result.put(attribute, brightnessValue(tileMap.get(token),bandsMap.get(token)));
			} else if (operation.equals("bandMeanAdd")) {
				String[] tokens1 = paramList.get(0).split("_");
				//TODO: consider different images
				int band1 = Integer.parseInt(tokens1[1].replace("layer",""))-1;
				String[] tokens2 = paramList.get(1).split("_");
				int band2 = Integer.parseInt(tokens2[1].replace("layer",""))-1;				
				bandArithmetic(tileMap.get(tokens1[0]), new int[] {band1, band2}, "Add", params);
				//result.put(attribute, bandArithmetic(tileMap.get(tokens1[0]), new int[] {band1, band2}, "Add"));
			} else if (operation.equals("bandMeanDiv")) {
				String[] tokens1 = paramList.get(0).split("_");
				//TODO: consider different images
				int band1 = Integer.parseInt(tokens1[1].replace("layer",""))-1;
				String[] tokens2 = paramList.get(1).split("_");
				int band2 = Integer.parseInt(tokens2[1].replace("layer",""))-1;				
				bandArithmetic(tileMap.get(tokens1[0]), new int[] {band1, band2}, "Div", params);
				//result.put(attribute, bandArithmetic(tileMap.get(tokens1[0]), new int[] {band1, band2}, "Div"));
			} else if (operation.equals("bandMeanMul")) {
				String[] tokens1 = paramList.get(0).split("_");
				//TODO: consider different images
				int band1 = Integer.parseInt(tokens1[1].replace("layer",""))-1;
				String[] tokens2 = paramList.get(1).split("_");
				int band2 = Integer.parseInt(tokens2[1].replace("layer",""))-1;				
				bandArithmetic(tileMap.get(tokens1[0]), new int[] {band1, band2}, "Mul", params);
				//result.put(attribute, bandArithmetic(tileMap.get(tokens1[0]), new int[] {band1, band2}, "Mul"));
			} else if (operation.equals("bandMeanSub")) {
				String[] tokens1 = paramList.get(0).split("_");
				//TODO: consider different images
				int band1 = Integer.parseInt(tokens1[1].replace("layer",""))-1;
				String[] tokens2 = paramList.get(1).split("_");
				int band2 = Integer.parseInt(tokens2[1].replace("layer",""))-1;				
				bandArithmetic(tileMap.get(tokens1[0]), new int[] {band1, band2}, "Sub", params);
				//result.put(attribute, bandArithmetic(tileMap.get(tokens1[0]), new int[] {band1, band2}, "Sub"));
			} else if (operation.equals("amplitudeValue")) {
				String[] tokens = paramList.get(0).split("_");
				int band = Integer.parseInt(tokens[1].replace("layer",""))-1;
				amplitudeValue(tileMap.get(tokens[0]), band, params);
				//result.put(attribute, amplitudeValue(tileMap.get(tokens[0]), band));
			} else if (operation.equals("standardDeviation")) {
				String[] tokens = paramList.get(0).split("_");
				int band = Integer.parseInt(tokens[1].replace("layer",""))-1;
				standardDeviation(tileMap.get(tokens[0]), band, params);
				//result.put(attribute, standardDeviation(tileMap.get(tokens[0]), band));
			}
						
			result.put(attribute, params);
			
		}
		
		return result;
	}
	
	@SuppressWarnings("unchecked")
	private void meanValue(Object obj, int band, Map<String, Object> params) {
		
		if (obj instanceof Map) {
			
			Map<String, Map<Integer, Object>> tiles = (Map<String, Map<Integer, Object>>)obj;
			
			if (tiles.size()==0) {
				//System.out.println("Tiles empty");
				return;				
			}
			
			double sum = 0.0;
			int count = 0;
			
			for (Map.Entry<String, Map<Integer, Object>> entry : tiles.entrySet()) {
								
				Map<Integer, Object> map = entry.getValue();
				
				Cursor <DoubleType> cursor = (Cursor <DoubleType>) map.get(band);
				
				cursor.reset();
				
				int c = (Integer)map.get(1000);	//code where we can find the area
				count = count + c;
				
				Sum< DoubleType, DoubleType >  res = new Sum< DoubleType, DoubleType >();
				
				double partial = res.compute(cursor, new DoubleType()).get();

				sum = sum + partial;
							
			}
				
			params.put("sum", sum);
			params.put("count", count);
			
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void standardDeviation(Object obj, int band, Map<String, Object> params) {
		
		if (obj instanceof Map) {
			
			Map<String, Map<Integer, Object>> tiles = (Map<String, Map<Integer, Object>>)obj;
			
			if (tiles.size()==0) {
				//System.out.println("Tiles empty");
				return;				
			}
			
			double squaredPixels = 0.0;
			double colorSum = 0.0;
			int area = 0;
			
			for (Map.Entry<String, Map<Integer, Object>> entry : tiles.entrySet()) {
				
				//String tile = entry.getKey();
				
				//System.out.println("Tile: " + tile);
				
				Map<Integer, Object> map = entry.getValue();
				
				Cursor <DoubleType> cursor = (Cursor <DoubleType>) map.get(band);
				
				cursor.reset();
				
				int c = (Integer)map.get(1000);	//code where we can find the area
				//area = area + c;
				
				double sp = 0.0;
				double cs = 0.0;
								
				while (cursor.hasNext()) {
					DoubleType value = cursor.next();
					double v = value.get();
					
					sp = sp + (v*v);
					cs = cs + v;					
				}
								
				squaredPixels = squaredPixels + sp;
				colorSum = colorSum + cs;
				area = area + c;
								
			}
			
			params.put("sum", colorSum);
			params.put("squared", squaredPixels);
			params.put("count", area);
						
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void maxPixelValue(Object obj, int band, Map<String, Object> params) {
		
		if (obj instanceof Map) {
			
			Map<String, Map<Integer, Object>> tiles = (Map<String, Map<Integer, Object>>)obj;
			
			if (tiles.size()==0) {
				//System.out.println("Tiles empty");
				return;				
			}
			
			double max = -Double.MAX_VALUE;			
			
			for (Map.Entry<String, Map<Integer, Object>> entry : tiles.entrySet()) {
				
				//String tile = entry.getKey();
				
				//System.out.println("Tile: " + tile);
				
				Map<Integer, Object> map = entry.getValue();
				
				Cursor <DoubleType> cursor = (Cursor <DoubleType>) map.get(band);
				
				cursor.reset();
				
				Max< DoubleType, DoubleType >  res = new Max< DoubleType, DoubleType >();
				
				double partial = res.compute(cursor, new DoubleType()).get();

				max = Math.max(max, partial);				
								
			}
			
			params.put("max", max);
						
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void minPixelValue(Object obj, int band, Map<String, Object> params) {
		
		if (obj instanceof Map) {
			
			Map<String, Map<Integer, Object>> tiles = (Map<String, Map<Integer, Object>>)obj;
			
			if (tiles.size()==0) {
				//System.out.println("Tiles empty");
				return;				
			}
			
			double min = Double.MAX_VALUE;			
			
			for (Map.Entry<String, Map<Integer, Object>> entry : tiles.entrySet()) {
				
				//String tile = entry.getKey();
				
				//System.out.println("Tile: " + tile);
				
				Map<Integer, Object> map = entry.getValue();
				
				Cursor <DoubleType> cursor = (Cursor <DoubleType>) map.get(band);
				
				cursor.reset();
				
				Min< DoubleType, DoubleType >  res = new Min< DoubleType, DoubleType >();
				
				double partial = res.compute(cursor, new DoubleType()).get();

				min = Math.min(min, partial);
								
			}
			
			params.put("min", min);
						
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void ratioValue(Object obj, int band, int bands, Map<String, Object> params) {
		
		if (obj instanceof Map) {
			
			Map<String, Map<Integer, Object>> tiles = (Map<String, Map<Integer, Object>>)obj;
			
			if (tiles.size()==0) {
				//System.out.println("Tiles empty");
				return;				
			}
			
			Double[] sum = new Double[bands];
			int count = 0;
					
			for (int b=0; b<bands; b++) {
				sum[b] = 0.0;
			}
			
			for (Map.Entry<String, Map<Integer, Object>> entry : tiles.entrySet()) {
				
				//String tile = entry.getKey();
				
				//System.out.println("Tile: " + tile);
				
				Map<Integer, Object> map = entry.getValue();
					
				int c = (Integer)map.get(1000);	//code where we can find the area
				count = count + c;
				
				for (int b=0; b<bands; b++) {

					Cursor <DoubleType> cursor = (Cursor <DoubleType>) map.get(b);
						
					cursor.reset();
					
					Sum< DoubleType, DoubleType >  res = new Sum< DoubleType, DoubleType >();
					
					double partial = res.compute(cursor, new DoubleType()).get();

					sum[b] = sum[b] + partial;
												
					//System.out.println("partial: " + partial/c + " - " + b);
			
				}
					
			}
			
			for (int i=0; i<bands; i++) {
				params.put("band_" + i, sum[i]);
			}
			
			params.put("band", band);
			params.put("bands", bands);
			params.put("count", count);
						
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void brightnessValue(Object obj, int bands, Map<String, Object> params) {
		
		if (obj instanceof Map) {
			
			Map<String, Map<Integer, Object>> tiles = (Map<String, Map<Integer, Object>>)obj;
			
			if (tiles.size()==0) {
				//System.out.println("Tiles empty");
				return;				
			}
			
			Double[] sum = new Double[bands];
			int count = 0;
			
			for (int b=0; b<bands; b++) {
				sum[b] = 0.0;
			}
						
			for (Map.Entry<String, Map<Integer, Object>> entry : tiles.entrySet()) {
				
				//String tile = entry.getKey();
				
				//System.out.println("Tile: " + tile);
				
				Map<Integer, Object> map = entry.getValue();
					
				int c = (Integer)map.get(1000);	//code where we can find the area
				count = count + c;
				
				for (int b=0; b<bands; b++) {

					Cursor <DoubleType> cursor = (Cursor <DoubleType>) map.get(b);
									
					cursor.reset();
					
					Sum< DoubleType, DoubleType >  res = new Sum< DoubleType, DoubleType >();
					
					double partial = res.compute(cursor, new DoubleType()).get();

					sum[b] = sum[b] + partial;
												
					//System.out.println("partial: " + partial/c + " - " + b);
			
				}
					
			}
			
			for (int i=0; i<bands; i++) {
				params.put("band_" + i, sum[i]);
			}

			params.put("bands", bands);
			params.put("count", count);
						
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void bandArithmetic(Object obj, int[] bands, String operation, Map<String, Object> params) {
		
		if (obj instanceof Map) {
			
			Map<String, Map<Integer, Object>> tiles = (Map<String, Map<Integer, Object>>)obj;
			
			if (tiles.size()==0) {
				//System.out.println("Tiles empty");
				return;				
			}
			
			Double[] sum = new Double[2];
			int count = 0;
			
			for (int b=0; b<2; b++) {
				sum[b] = 0.0;
			}
						
			for (Map.Entry<String, Map<Integer, Object>> entry : tiles.entrySet()) {
				
				//String tile = entry.getKey();
				
				//System.out.println("Tile: " + tile);
			
				Map<Integer, Object> map = entry.getValue();
					
				int c = (Integer)map.get(1000);	//code where we can find the area
				count = count + c;
				
				for (int b=0; b<2; b++) {

					Cursor <DoubleType> cursor = (Cursor <DoubleType>) map.get(bands[b]);
							
					cursor.reset();
					
					Sum< DoubleType, DoubleType >  res = new Sum< DoubleType, DoubleType >();
					
					double partial = res.compute(cursor, new DoubleType()).get();

					sum[b] = sum[b] + partial;
			
				}
					
			}
						
			params.put("band_a", sum[0]);
			params.put("band_b", sum[1]);
			params.put("count", count);
						
		}
		
	}
	
	@SuppressWarnings("unchecked")
	private void amplitudeValue(Object obj, int band, Map<String, Object> params) {
		
		if (obj instanceof Map) {
			
			Map<String, Map<Integer, Object>> tiles = (Map<String, Map<Integer, Object>>)obj;
			
			if (tiles.size()==0) {
				//System.out.println("Tiles empty");
				return;				
			}
			
			double min = Double.MAX_VALUE;
			double max = -Double.MAX_VALUE;
			
			for (Map.Entry<String, Map<Integer, Object>> entry : tiles.entrySet()) {
				
				//String tile = entry.getKey();
				
				//System.out.println("Tile: " + tile);
				
				Map<Integer, Object> map = entry.getValue();
				
				Cursor <DoubleType> cursor = (Cursor <DoubleType>) map.get(band);
				
				cursor.reset();
				
				Min< DoubleType, DoubleType >  resMin = new Min< DoubleType, DoubleType >();
				Max< DoubleType, DoubleType >  resMax = new Max< DoubleType, DoubleType >();
				
				double partialMin = resMin.compute(cursor, new DoubleType()).get();
				
				cursor.reset();
				
				double partialMax = resMax.compute(cursor, new DoubleType()).get();

				min = Math.min(min, partialMin);
				max = Math.max(max, partialMax);
				
			}
			
			params.put("max", max);
			params.put("min", min);
			
		}
		
	}
		
}
