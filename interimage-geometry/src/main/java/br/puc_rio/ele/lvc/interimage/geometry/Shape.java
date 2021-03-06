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

package br.puc_rio.ele.lvc.interimage.geometry;

/**
 * A class that holds the information about a shape resource. 
 * @author Rodrigo Ferreira
 */
public class Shape {

	private String _key;
	private String _url;
	private String _crs;
	private boolean _splittable;
	
	public void setKey(String key) {
		_key = key;
	}
	
	public String getKey() {
		return _key;
	}
	
	public void setURL(String url) {
		_url = url;
	}
	
	public String getURL() {
		return _url;
	}
	
	public void setCRS(String crs) {
		_crs = crs;
	}
	
	public String getCRS() {
		return _crs;
	}
	
	public void isSplittable(boolean splittable) {
		_splittable = splittable;
	}
	
	public boolean isSplittable() {
		return _splittable;
	}
		
}
