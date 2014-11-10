package br.puc_rio.ele.lvc.interimage.core.operatorgraph;

import java.util.ArrayList;
import java.util.List;

public abstract class gOperator extends gNode {
	
	//private DefaultDirectedGraph<gNode, gEdge> graph_;
	
	private List<String> inputClasses_ = new ArrayList<String>();
	private List<String> outputClasses_ = new ArrayList<String>();
	
	//private List<String> inputPaths_ = new ArrayList<String>();
	//private List<String> outputPaths_ = new ArrayList<String>();
	
	//private String outputPath_ = new String();
	
	/*public void setGraph(DefaultDirectedGraph<gNode, gEdge> graph) {
		graph_ = graph;
	}
	
	public DefaultDirectedGraph<gNode, gEdge> getGraph() {
		return graph_;
	}*/
		
	/*Paths*/
	
	/*public void setOutputPath(String path) {
		outputPath_ = path;
	}
	
	public String getOutputPath() {
		return outputPath_;
	}*/
	
	/*public List<String> getInputPaths() {
		return inputPaths_;
	}
	
	public void setInputPaths(List<String> inputPaths_) {
		this.inputPaths_ = inputPaths_;
	}
	
	public List<String> getOutputPaths() {
		return outputPaths_;
	}
	
	public void setOutputPaths(List<String> outputPaths_) {
		this.outputPaths_ = outputPaths_;
	}
	
	public void addInputPath(String path) {
		inputPaths_.add(path);
	}
	
	public void addOutputPath(String path) {
		outputPaths_.add(path);
	}*/
	
	/*Classes*/
	
	public List<String> getInputClasses() {
		return inputClasses_;
	}
		
	public void setInputClasses(List<String> inputClasses_) {
		this.inputClasses_ = inputClasses_;
	}
	
	public List<String> getOutputClasses() {
		return outputClasses_;
	}
	
	public void setOutputClasses(List<String> outputClasses_) {
		this.outputClasses_ = outputClasses_;
	}
	
	public void addInputClass(String classe) {
		inputClasses_.add(classe);
	}
	
	public void addOutputClass(String classe) {
		outputClasses_.add(classe);
	}
	
	public boolean hasInputClass(String classe) {
		return inputClasses_.contains(classe);
	}
	
	public boolean hasOutputClass(String classe) {
		return outputClasses_.contains(classe);
	}
		
}
