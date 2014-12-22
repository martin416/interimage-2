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

package br.puc_rio.ele.lvc.interimage.core.clustermanager;

import java.util.Properties;

/**
 * An interface for the cluster manager. 
 * @author Rodrigo Ferreira
 */

public interface ClusterManager {

	public void setProperties(Properties props);
	
	public String createCluster();
	
	public ExecutionTracker runPigScript(String script, String clusterId);
	
	public void terminateCluster(String clusterId);
	
}
