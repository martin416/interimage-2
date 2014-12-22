package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import br.puc_rio.ele.lvc.interimage.core.clustermanager.ClusterManager;
import br.puc_rio.ele.lvc.interimage.core.clustermanager.ExecutionTracker;

public class gII1Operator extends gOperator {

	private String II1Executable_;
	private String projectFile_;
	private String resourceImage_;
	private String resourceShape_;
	private String outputDecisionRule_;
	private String outputShapeFile_;
	private Map<String,String> parameterList_ = new HashMap<String,String>();
	
	//TODO: verify if this method is compliant with the new ExecutionTracker mechanism
	
	@Override	
	protected ExecutionTracker execute(ClusterManager clusterManager, String clusterId, boolean setup) {
		// TODO Auto-generated method stub
		String command = buildCommand();
		
		Runtime rt = Runtime.getRuntime();
		
		try {
			Process pr = rt.exec(command);
			int value = pr.waitFor();
			return null;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	private String buildCommand() {

		String command = II1Executable_+ " " + "projectFile=" + projectFile_ + " " ;
		command = command + "resourceImage=" + resourceImage_ + " ";
		command = command + "resourceShape=" + resourceShape_ + " ";
		command = command + "outputDecisionRule=" + outputDecisionRule_ + " ";
		command = command + "outputShapeFile=" + outputShapeFile_ + " ";
		
		for (Map.Entry<String, String> entry : this.parameterList_.entrySet()) {
			command = command + "parameterOperator=@" + entry.getKey() + "@#" + entry.getValue() + " ";			
		}
				
		return command;
	}

	public String getProjectFile() {
		return projectFile_;
	}

	public void setProjectFile(String projectFile) {
		this.projectFile_ = projectFile;
	}

	public String getResourceImage() {
		return resourceImage_;
	}

	public void setResourceImage(String resourceImage) {
		this.resourceImage_ = resourceImage;
	}

	public String getResourceShape() {
		return resourceShape_;
	}

	public void setResourceShape(String resourceShape) {
		this.resourceShape_ = resourceShape;
	}

	public String getOutputDecisionRule() {
		return outputDecisionRule_;
	}

	public void setOutputDecisionRule(String outputDecisionRule) {
		this.outputDecisionRule_ = outputDecisionRule;
	}

	public String getOutputShapeFile() {
		return outputShapeFile_;
	}

	public void setOutputShapeFile(String outputShapeFile) {
		this.outputShapeFile_ = outputShapeFile;
	}

	public void addParamater(String name, String value) {
		this.parameterList_.put(name, value);
	}
	
	public void changeParamater(String name, String value) {
		this.parameterList_.put(name, value);
	}

	public String getII1Executable() {
		return II1Executable_;
	}

	public void setII1Executable(String II1Executable) {
		this.II1Executable_ = II1Executable;
	}

	@Override
	public JSONObject exportToJSON() {
		JSONObject my_obj = new JSONObject();
		
		my_obj.put("type", "gII1Operator");
				
		my_obj.put("II1Executable", II1Executable_);
		my_obj.put("projectFile", projectFile_);
		my_obj.put("resourceImage", resourceImage_);
		my_obj.put("resourceShape", resourceShape_);
		my_obj.put("outputDecisionRule", outputDecisionRule_);
		my_obj.put("outputShapeFile", outputShapeFile_);
		
		JSONArray my_obj2 = new JSONArray();
		
		for (Map.Entry<String, String> entry : this.parameterList_.entrySet()) {
			JSONObject my_obj3 = new JSONObject();
			my_obj3.put(entry.getKey(), entry.getValue());
			my_obj2.put(my_obj3);
			my_obj3=null;
		}
		
		my_obj.put("parametersOperator", my_obj2);
				
		return my_obj;
	}

	@Override
	public Boolean importFromJSON(JSONObject obj) {
		
		if (obj.getString("II1Executable")=="")
			return false;
		else {
			this.setII1Executable(obj.getString("II1Executable"));
		}
		if (obj.getString("projectFile")=="")
			return false;
		else {
			this.setProjectFile(obj.getString("projectFile"));
		}
		this.setResourceImage(obj.getString("resourceImage"));
		this.setResourceShape(obj.getString("resourceShape"));
		this.setOutputDecisionRule(obj.getString("outputDecisionRule"));
		this.setOutputShapeFile(obj.getString("outputShapeFile"));
		
		JSONArray my_obj2 = new JSONArray(obj.getString("parametersOperator"));
		
		for (int i = 0; i < my_obj2.length(); i++) {
			JSONObject my_param = new JSONObject(my_obj2.get(i));
			String key = my_param.keys().next().toString();
			this.addParamater(key, my_param.getString(key));
		}
		
		return true;
	}
	
}

//1) Segmenta����o

//interimage 
//"projectFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\segmentation\segmentation.gap" 
//"resourceImage=image#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.tif" 
//"resourceShape=roi#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.shp" 
//"outputDecisionRule=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.dt" 
//"outputShapeFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\segmentation\result.shp" 
//"parameterOperator=@resolution@#1" 
//"parameterOperator=@bands@#0,1,2,3" 
//"parameterOperator=@weights@#1,1,1,1" 
//"parameterOperator=@compactness@#0.5" 
//"parameterOperator=@color@#0.5" 
//"parameterOperator=@scale@#50"

//2) Limiariza����o

//interimage 
//"projectFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\thresholding\thresholding.gap" 
//"resourceImage=image#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.tif" 
//"resourceShape=roi#C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.shp" 
//"outputDecisionRule=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\default.dt" 
//"outputShapeFile=C:\Users\Rodrigo\Documents\interimage-2\interimage1_projects\thresholding\result.shp" 
//"parameterOperator=@resolution@#1" 
//"parameterOperator=@min@#0" 
//"parameterOperator=@max@#0.33" 
//"parameterOperator=@expression@#(R0:3 - R0:0) / (R0:3 + R0:0)"