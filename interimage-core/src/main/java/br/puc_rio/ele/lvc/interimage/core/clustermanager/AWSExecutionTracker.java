package br.puc_rio.ele.lvc.interimage.core.clustermanager;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepExecutionState;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;

public class AWSExecutionTracker implements ExecutionTracker {

	AddJobFlowStepsResult _result;
	String _clusterId;
	AmazonElasticMapReduce _emr;
		
	private static final List<StepExecutionState> STEP_DONE_STATES = Arrays
	        .asList(new StepExecutionState[] { StepExecutionState.COMPLETED,
	        									StepExecutionState.FAILED,
	        									StepExecutionState.CANCELLED,
	        									StepExecutionState.INTERRUPTED});
	
	public void setJobFlowStepsResult(AddJobFlowStepsResult result) {
		_result = result;
	}
	
	public void setClusterId(String clusterId) {
		_clusterId = clusterId;
	}
	
	public void setAmazonElasticMapReduce(AmazonElasticMapReduce emr) {
		_emr = emr;
	}
	
	public String getStatus() {
		DescribeStepRequest desc = new DescribeStepRequest().withClusterId(_clusterId).withStepId(_result.getStepIds().get(_result.getStepIds().size()-1));
		DescribeStepResult descResult = _emr.describeStep(desc);
        
    	Step step = descResult.getStep();
    	
    	StepStatus status = step.getStatus();
    	
    	return status.toString();
	}
	
	public boolean isDone() {
		// TODO Auto-generated method stub
		
		try {
			
			//Check the status of the step
	        //String lastState = "";
	        //STATUS_LOOP: while (true)
	        //{
	        	DescribeStepRequest desc = new DescribeStepRequest().withClusterId(_clusterId).withStepId(_result.getStepIds().get(_result.getStepIds().size()-1));
	        	
				DescribeStepResult descResult = _emr.describeStep(desc);
	            
	        	Step step = descResult.getStep();
	        	
	        	StepStatus status = step.getStatus();
                
	            String state = status.getState();
                if (stepIsDone(state))
                {
                    //System.out.println("Step " + state + ": " + status.toString());
                    return true;
                }
                /*else if (!lastState.equals(state))
                {
                    lastState = state;
                    //System.out.println("Step " + state + " at " + new Date().toString());
                }*/
	            
	            //Thread.sleep(10000);
	        	
	        //}
			                
		} catch (Exception e) {
			System.out.print(e.getMessage());			
			e.printStackTrace();
		}
		
		return false;
		
	}

	public static boolean stepIsDone(String value)
    {
        StepExecutionState state = StepExecutionState.fromValue(value);
        return STEP_DONE_STATES.contains(state);
    }
	
}
