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

package br.puc_rio.ele.lvc.interimage.core.project;

import br.puc_rio.ele.lvc.interimage.common.URL;
import br.puc_rio.ele.lvc.interimage.core.datamanager.DataManager;
import br.puc_rio.ele.lvc.interimage.core.datamanager.DefaultResource;
import br.puc_rio.ele.lvc.interimage.core.datamanager.SplittableResource;
import br.puc_rio.ele.lvc.interimage.core.semanticnetwork.SemanticNetwork;
import br.puc_rio.ele.lvc.interimage.data.Image;
import br.puc_rio.ele.lvc.interimage.data.ImageList;
import br.puc_rio.ele.lvc.interimage.datamining.FuzzySet;
import br.puc_rio.ele.lvc.interimage.datamining.FuzzySetList;
import br.puc_rio.ele.lvc.interimage.geometry.Shape;
import br.puc_rio.ele.lvc.interimage.geometry.ShapeList;
import br.puc_rio.ele.lvc.interimage.geometry.TileManager;
import br.puc_rio.ele.lvc.interimage.geometry.UTMLatLongConverter;
import br.puc_rio.ele.lvc.interimage.geometry.WebMercatorLatLongConverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * A class that holds the information about an interpretation project. 
 * @author Rodrigo Ferreira
 * 
 * TODO: preprocess net file: 1) replace "topdown/", 2) get rid of decision rules and 3) set epsg code 		 
 */
public class Project {

	private String _project;
	private SemanticNetwork _semanticNet;
	private ImageList _imageList;
	private ShapeList _shapeList;
	private DataManager _dataManager;
	private double _minResolution;
	//TODO: Make it a parameter
	private int _tilePixelSize;
	private TileManager _tileManager;
	private FuzzySetList _fuzzySetList;
	//private String _decisionTree = null;
	private Properties _properties;
	
	public Project() {
		_semanticNet = new SemanticNetwork();
		_imageList = new ImageList();
		_shapeList = new ShapeList();
		_dataManager = new DataManager();
		_fuzzySetList = new FuzzySetList();
		_minResolution = Double.MAX_VALUE;
		_properties = new Properties();
	}
	
	public String getProject() {
		return _project;
	}
	
	public SemanticNetwork getSemanticNetwork() {
		return _semanticNet;
	}
	
	public ImageList getImageList() {
		return _imageList;
	}
	
	public ShapeList getShapeList() {
		return _shapeList;
	}
	
	public void readOldFile(String url) {
	
		try {
		
			/*Processing input parameters*/
			if (url == null) {
	            throw new Exception("No project file specified");
	        } else {
	        	if (url.isEmpty()) {
	        		throw new Exception("No project file specified");
	        	}
	        }
						
			_project = url;
			
			/*Reading properties file*/
			InputStream input = new FileInputStream("interimage.properties");

			_properties.load(input);

			_tilePixelSize = Integer.parseInt(_properties.getProperty("interimage.tileSize"));
			
			_dataManager.setup(_properties);
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			
			InputStream in = new FileInputStream(url);
			
			Document doc = dBuilder.parse(in);
		      
		    Element rootElement = doc.getDocumentElement();
		    rootElement.normalize();
			
		    if (rootElement.getNodeName() == "geoproject") {
		    			    	
		    	NodeList semNets = rootElement.getElementsByTagName("geosemnet");
			    NodeList images = rootElement.getElementsByTagName("image");
			    NodeList shapes = rootElement.getElementsByTagName("shape");
			    NodeList fuzzySets = rootElement.getElementsByTagName("fuzzysets");			    
		    				    
			    /*Reading Semantic Network*/
			    if (semNets.getLength() > 0) {
			    	
			    	Element semNet = (Element)semNets.item(0);
			    	
			    	_semanticNet.readOldFile(semNet.getAttribute("dir") + File.separatorChar + semNet.getAttribute("file"));
			    	
			    	//TODO: send semantic network to AWS
			    	
			    } else {
			    	throw new Exception("No geosemnet tag defined");
			    }
			    
			    /*Reading Image List*/
			    if (images.getLength() > 0) {
			    				    	
			    	for (int k = 0; k < images.getLength(); k++) {
				    	
			    		Node imageNode = images.item(k);
			    					    		
			    		if (imageNode.getNodeType() == Node.ELEMENT_NODE) {
			    			
				    		Element image = (Element)imageNode;
				    		
					    	Image img = new Image();
					    	String key = image.getAttribute("key");				    	
					    	
					    	img.setKey(key);
					    	img.setDefault(Boolean.parseBoolean(image.getAttribute("default")));
					    	img.setURL(image.getAttribute("file"));
						    					    	
					    	String epsgFrom = image.getAttribute("epsg");
					    	
					    	img.setEPSG(epsgFrom);
					    	
					    	//TODO: Check if this conversion can be done somewhere else
					    	
					    	int epsgFromCode = Integer.parseInt(epsgFrom.split(":")[1]);
					    						    								
							Coordinate coord1 = new Coordinate(Double.parseDouble(image.getAttribute("geoWest")), Double.parseDouble(image.getAttribute("geoSouth")));
							
							Coordinate coord2 = new Coordinate(Double.parseDouble(image.getAttribute("geoEast")), Double.parseDouble(image.getAttribute("geoNorth")));
							
					    	if (epsgFromCode == 4326) {
					    	
					    		WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
								webMercator.setDatum("WGS84");
					    		
					    		webMercator.LatLongToWebMercator(coord1);
					    		webMercator.LatLongToWebMercator(coord2);
								
					    	} else if (((epsgFromCode >= 32601) && (epsgFromCode <= 32660)) || ((epsgFromCode >= 32701) && (epsgFromCode <= 32760))) {
					    	
					    		int utmZone = (epsgFromCode>32700) ? epsgFromCode-32700 : epsgFromCode-32600;
								boolean southern = (epsgFromCode>32700) ? true : false;
						    
								UTMLatLongConverter utm = new UTMLatLongConverter();
								utm.setDatum("WGS84");
								
								WebMercatorLatLongConverter webMercator = new WebMercatorLatLongConverter();
								webMercator.setDatum("WGS84");
								
								utm.UTMToLatLong(coord1, utmZone, southern);
						    	webMercator.LatLongToWebMercator(coord1);
						    	
						    	utm.UTMToLatLong(coord2, utmZone, southern);
						    	webMercator.LatLongToWebMercator(coord2);
			                	  		
					    	}
					    		
					    	img.setGeoWest(coord1.x);
					    	img.setGeoNorth(coord2.y);
					    	img.setGeoEast(coord2.x);
					    	img.setGeoSouth(coord1.y);
					    						    	
					    	img.setCols(Integer.parseInt(image.getAttribute("cols")));
					    	img.setRows(Integer.parseInt(image.getAttribute("rows")));
					    	img.setBands(Integer.parseInt(image.getAttribute("bands")));
					    		
					    	double res = Math.abs((img.getGeoEast()-img.getGeoWest())/img.getCols());
					    						    	
					    	if (res < _minResolution) {
					    		_minResolution = res; 
					    	}
					    						    	
					    	_imageList.add(key, img);
					    	
			    		}
			    	
			    	}
			    				    	
			    	_tileManager = new TileManager(_tilePixelSize * _minResolution);
			    	
			    	_dataManager.updateGeoBBox(new double[] {_imageList.getGeoWest(), _imageList.getGeoSouth(), _imageList.getGeoEast(), _imageList.getGeoNorth()}); 
			    	
			    	for (Map.Entry<String, Image> entry : _imageList.getImages().entrySet()) {
			    		_dataManager.setupResource(new SplittableResource(entry.getValue(),SplittableResource.IMAGE), _tileManager, null);
			    	}
			    				    	
			    } else {
			    	throw new Exception("No image tag defined");
			    }
			    			    
			    /*Reading Shape List*/
			    if (shapes.getLength() > 0) {
			    				    	
			    	for (int k = 0; k < shapes.getLength(); k++) {
				    	Element shape = (Element)shapes.item(k);
				    	
				    	Shape shp = new Shape();
				    	String key = shape.getAttribute("key");				    	
				    	
				    	shp.setKey(key);
				    	shp.setURL(shape.getAttribute("file"));
				    	
				    	String epsgFrom = shape.getAttribute("epsg");				    	
				    	shp.setEPSG(epsgFrom);
				    	
				    	Boolean splittable = new Boolean(shape.getAttribute("splittable"));				    	
				    	shp.isSplittable(splittable);
				    	
				    	_shapeList.add(key, shp);
				    	
				    	if (splittable) {
				    		_dataManager.setupResource(new SplittableResource(shp,SplittableResource.SHAPE), _tileManager, null);
				    	} else {
				    		_dataManager.setupResource(new DefaultResource(shp,DefaultResource.SHAPE), null, null);
				    	}
			    	}
			    				    	
			    } else {
			    	throw new Exception("No shape tag defined");
			    }
			    	
			    /*Creating Tiles*/
			    _tileManager.setTiles(_dataManager.getGeoBBox());
			    _dataManager.setupResource(new DefaultResource(_tileManager.getTiles(), DefaultResource.TILE), null, URL.getPath(_project));
			    			    
			    /*Reading FuzzySets*/
			    if (fuzzySets.getLength() > 0) {
			    	
			    	Element fuzzySet = (Element)fuzzySets.item(0);
			    	
			    	_fuzzySetList.readOldFile(fuzzySet.getAttribute("file"));

			    	if (_fuzzySetList.size()>0)
			    		_dataManager.setupResource(new DefaultResource(new ArrayList<FuzzySet>(_fuzzySetList.getFuzzySets().values()), DefaultResource.FUZZY_SET), null, URL.getPath(_project));			    	
			    	
			    } else {
			    	throw new Exception("No fuzzysets tag defined");
			    }
						    
		    } else {
		    	throw new Exception("No geoproject tag defined");
		    }
		    
		} catch (Exception e) {
			System.err.println("Failed to read project file; error - " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
}
