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

package br.puc_rio.ele.lvc.interimage.common;

import com.vividsolutions.jts.index.strtree.AbstractNode;
import com.vividsolutions.jts.index.strtree.STRtree;

/**
 * Provides a proxy for the STRtree class that exposes some protected methods.
 * @author Rodrigo Ferreira
 *
 */
@SuppressWarnings("serial")
public class SpatialIndex extends STRtree {
		
	@Override
	public AbstractNode getRoot() {
		return super.getRoot();
	}
	
}
