package br.puc_rio.ele.lvc.interimage.common;

import java.util.ArrayList;
import java.util.List;

/**
 * A fixed grid tile manager implementation.
 * @author Rodrigo Ferreira
 *
 */

public class FixedGridTileManager implements TileManager {

	private double _tileSize; 
	private int _numTilesX;
	private int _numTilesY;
	private double[] _worldBBox;
	private List<Tile> _tiles;	
	private String _crs;
	
	
	public FixedGridTileManager(double size, String crs) {
		setSize(size, crs);
		_tiles = new ArrayList<Tile>();
	}
	
	private void setSize(double size, String crs) {
		_tileSize = size;
		
		_crs = crs;
		
		double[] bounds = new CRS().getBounds(_crs); 
		
		_numTilesX = (int)Math.ceil((bounds[2]-bounds[0]) / _tileSize);
		_numTilesY = (int)Math.ceil((bounds[3]-bounds[1]) / _tileSize);
		
		_worldBBox = new double[] {bounds[0], bounds[1], bounds[2], bounds[3]}; 
				
	}
	
	public String encode(long id) {
		return "T" + id;
	}
	
	public String encode(int i, int j) {
		long id = ((long)j)*_numTilesX+i+1;
		return encode(id);
	}
	
	public List<String> getTiles(double[] bbox) {
        		
		int tileMinX = (int)Math.floor((bbox[0]-_worldBBox[0]) / _tileSize);
        int tileMinY = (int)Math.floor((bbox[1]-_worldBBox[1]) / _tileSize);
        int tileMaxX = (int)Math.floor((bbox[2]-_worldBBox[0]) / _tileSize);
        int tileMaxY = (int)Math.floor((bbox[3]-_worldBBox[1]) / _tileSize);
                
        ArrayList<String> list = new ArrayList<String>();
        
        for (int j=tileMinY; j<=tileMaxY; j++) {
        	for (int i=tileMinX; i<=tileMaxX; i++) {
        		long id = ((long)j)*_numTilesX+i+1;
        		list.add("T" + id);
        	}
        }
        
        return list;
        
	}
	
	public int[] getTileCoordinates(double[] bbox) {
		int[] tileCoords = new int[4];
		tileCoords[0] = (int)Math.floor((bbox[0]-_worldBBox[0]) / _tileSize);
		tileCoords[1] = (int)Math.floor((bbox[1]-_worldBBox[1]) / _tileSize);
		tileCoords[2] = (int)Math.floor((bbox[2]-_worldBBox[0]) / _tileSize);
		tileCoords[3] = (int)Math.floor((bbox[3]-_worldBBox[1]) / _tileSize);
		
		return tileCoords;
	}
	
	public int getNumTilesX() {
		return _numTilesX;
	}
	
	public int getNumTilesY() {
		return _numTilesY;
	}
	
	public double getTileSize() {
		return _tileSize;
	}
	
	public void setTiles(double[] geoBBox) {
				
		int[] tileCoords = getTileCoordinates(geoBBox);
				
		for (int j=tileCoords[1]; j<=tileCoords[3]; j++) {
			for (int i=tileCoords[0]; i<=tileCoords[2]; i++) {
				Tile tile = new Tile();
				long id = ((long)j)*_numTilesX+i+1;
				tile.setId(id);
				tile.setCode("T" + id);
				
				double geoX = i*_tileSize + _worldBBox[0];
				double geoY = j*_tileSize + _worldBBox[1];
				
				tile.setGeometry(String.format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", geoX, geoY, geoX + _tileSize, geoY, geoX + _tileSize, geoY + _tileSize, geoX, geoY + _tileSize, geoX, geoY));
								
				_tiles.add(tile);
			}
		}
		
	}
	
	/*public Geometry getTileGeometry(String tile) {
		
		Geometry geometry = null;
		
		try {
		
			long idx = Long.parseLong(tile.substring(1)) - 1;
			
			int i = (int)(idx % _numTilesX);
			int j = (int)(idx / _numTilesX);
			
			double geoX = i*_tileSize + _worldBBox[0];
			double geoY = j*_tileSize + _worldBBox[1];
			
			String geomStr = String.format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", geoX, geoY, geoX + _tileSize, geoY, geoX + _tileSize, geoY + _tileSize, geoX, geoY + _tileSize, geoX, geoY);
			
			geometry = new WKTReader().read(geomStr);
			
		} catch (Exception e) {
			System.out.println("Failed to compute geometry from id: " + e.getMessage());
		}
		
		return geometry;
		
	}*/
			
	public List<String> getNeighourTiles(String code, List<String> directions) {
		
		List<String> neighbours = new ArrayList<String>();
		
		long id = Long.parseLong(code.substring(1)); 
		
		int i = (int)(id-1) % _numTilesX;
		int j = (int)(id-1) / _numTilesX;
		
		//N
		if (directions.contains("N")) {
			if (j+1<_numTilesY) {
				neighbours.add(encode(i, j+1));				
			}
		}
		
		//NE
		if (directions.contains("NE")) {
			if ((j+1<_numTilesY) && (i+1<_numTilesX)) {
				neighbours.add(encode(i+1, j+1));
			}
		}		
				
		//E
		if (directions.contains("E")) {
			if (i+1<_numTilesX) {
				neighbours.add(encode(i+1, j));
			}
		}
		
		//SE
		if (directions.contains("SE")) {
			if ((j-1>=0) && (i+1<_numTilesX)) {
				neighbours.add(encode(i+1, j-1));
			}
		}
		
		//S
		if (directions.contains("S")) {
			if (j-1>=0) {
				neighbours.add(encode(i, j-1));
			}
		}
		
		//SW
		if (directions.contains("SW")) {
			if ((j-1>=0) && (i-1>=0)) {
				neighbours.add(encode(i-1, j-1));
			}
		}
		
		//W
		if (directions.contains("W")) {
			if (i-1>=0) {
				neighbours.add(encode(i-1, j));
			}
		}
		
		//NW
		if (directions.contains("NW")) {
			if ((j+1<_numTilesY) && (i-1>=0)) {
				neighbours.add(encode(i-1, j+1));
			}
		}
		
		return neighbours;
	}
	
	public String getCRS() {
		return _crs;
	}
	
	public List<Tile> getTiles() {
		return _tiles;
	}
	
	public double[] getWorldBBox() {
		return _worldBBox;
	}
	
}


