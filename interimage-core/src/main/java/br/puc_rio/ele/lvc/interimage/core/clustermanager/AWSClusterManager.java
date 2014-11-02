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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import br.puc_rio.ele.lvc.interimage.core.datamanager.AWSSource;
import br.puc_rio.ele.lvc.interimage.core.datamanager.DefaultResource;
import br.puc_rio.ele.lvc.interimage.core.datamanager.Source;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowExecutionState;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

/**
 * Implementation of the cluster manager for Amazon. 
 * @author Rodrigo Ferreira
 */

public class AWSClusterManager implements ClusterManager {

	private AmazonElasticMapReduce emr_;
	private Source source_;
	private Properties properties_;
	
	private static final List<JobFlowExecutionState> DONE_STATES = Arrays
	        .asList(new JobFlowExecutionState[] { JobFlowExecutionState.COMPLETED,
	                                             JobFlowExecutionState.FAILED,
	                                             JobFlowExecutionState.TERMINATED,
	                                             JobFlowExecutionState.WAITING});
	
	public void setProperties(Properties props) {
		properties_ = props;
	}
	
	public String createCluster() {
		
		System.out.println("Creating cluster...");
						
		AWSCredentials credentials = new BasicAWSCredentials(properties_.getProperty("interimage.aws.accessKey"), properties_.getProperty("interimage.aws.secretKey"));
		
		//Sending credentials to AWS EMR in order to make API calls
		emr_ = new AmazonElasticMapReduceClient(credentials);
		
		source_ = new AWSSource(properties_.getProperty("interimage.aws.accessKey"),properties_.getProperty("interimage.aws.secretKey"),properties_.getProperty("interimage.aws.S3Bucket"), true);
		
		String debugging = properties_.getProperty("interimage.aws.debugging");
		String instanceType = properties_.getProperty("interimage.aws.instanceType");
		String market = properties_.getProperty("interimage.aws.market");
		String bidPrice = properties_.getProperty("interimage.aws.bidPrice");
		String hadoopVersion = properties_.getProperty("interimage.aws.hadoopVersion");
		String region = properties_.getProperty("interimage.aws.region");
		
		StepFactory stepFactory = new StepFactory();
		
		StepConfig enableDebugging = null;
		
		if (debugging.equals("true")) {
			
			System.out.println("Debugging enabled...");
			
			enableDebugging = new StepConfig()
		    .withName("Enable debugging")
		    .withActionOnFailure("CANCEL_AND_WAIT")
		    .withHadoopJarStep(stepFactory.newEnableDebuggingStep());
		}
		
		//For now Pig, but other languages like Hive could be used
		StepConfig installPig = new StepConfig()
	    .withName("Install Pig")
	    .withActionOnFailure("CANCEL_AND_WAIT")
	    .withHadoopJarStep(stepFactory.newInstallPigStep());
		
		//Configuring master instance
		InstanceGroupConfig instanceMaster = new InstanceGroupConfig()
		.withMarket(market)
		.withInstanceType(instanceType)
		.withInstanceRole("MASTER")
		.withInstanceCount(1)
		.withBidPrice(bidPrice);
		
		//Configuring slave instances		
		InstanceGroupConfig instanceSlave = new InstanceGroupConfig()
		.withMarket(market)
		.withInstanceType(instanceType)
		.withInstanceRole("CORE")
		.withInstanceCount(Integer.parseInt(properties_.getProperty("interimage.clusterSize"))-1)
		.withBidPrice(bidPrice);		
		
		//General job flow settings		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
        //.withEc2KeyName("keypair")
        .withHadoopVersion(hadoopVersion)
        .withInstanceGroups(instanceMaster, instanceSlave)
        //.withInstanceCount(Integer.parseInt(properties_.getProperty("interimage.clusterSize")))
        .withKeepJobFlowAliveWhenNoSteps(true)
        .withPlacement(new PlacementType(region));
        //.withMasterInstanceType("m1.xlarge")
        //.withSlaveInstanceType("m1.xlarge");
		
		RunJobFlowRequest request = new RunJobFlowRequest()
	    .withName("Pig Interactive")
	    //.withAmiVersion("3.1.0")
	    .withLogUri("s3://" + properties_.getProperty("interimage.aws.S3Bucket") + "/interimage/" + properties_.getProperty("interimage.projectName"))	    	
	    //.withVisibleToAllUsers(true)
	    .withInstances(instances);
		
		if (debugging.equals("true")) {
			request.setSteps(Arrays.asList(enableDebugging, installPig));
		} else {
			request.setSteps(Arrays.asList(installPig));
		}
		
		RunJobFlowResult result = emr_.runJobFlow(request);
		
		return result.getJobFlowId();
		
	}
	
	public void runPigScript(String script, String clusterId) {
		
		System.out.println("Running pig script...");
		
		StepFactory stepFactory = new StepFactory();
	    
		Random randomGenerator = new Random();
		
		String name = randomGenerator.nextInt(100000) + ".pig";
		
		String to = "interimage/" + properties_.getProperty("interimage.projectName") + "/" + name;
		
		System.out.println("Uploading script to: " + to);
		
		try {
			File temp = File.createTempFile("script",".pig");
			temp.deleteOnExit();
			BufferedWriter out = new BufferedWriter(new FileWriter(temp));
		    out.write(script);
		    out.close();
		    
		    System.out.println("Uploading...");
		    
		    source_.put(temp.getAbsolutePath(), to, new DefaultResource(new String(name), DefaultResource.FILE));
		    		    
		} catch (Exception e) {
			System.out.println("It was not possible to upload the Pig script.");
			e.printStackTrace();
		}
				
		StepConfig runPigScript = new StepConfig()
	    .withName("Run Pig script")
	    .withActionOnFailure("CANCEL_AND_WAIT") //verify other options
	    .withHadoopJarStep(stepFactory.newRunPigScriptStep("s3://" + properties_.getProperty("interimage.aws.S3Bucket") + "/" + to));
		
		//System.out.println("s3://" + properties_.getProperty("interimage.aws.S3Bucket") + "/" + to);
		
		//AddJobFlowStepsResult result =
		emr_.addJobFlowSteps(new AddJobFlowStepsRequest().withJobFlowId(clusterId).withSteps(runPigScript));
		
		try {
		
			//Check the status of the running job
	        String lastState = "";
	        STATUS_LOOP: while (true)
	        {
	            DescribeJobFlowsRequest desc =
	                new DescribeJobFlowsRequest(
	                                            Arrays.asList(new String[] { clusterId }));
	            @SuppressWarnings("deprecation")
				DescribeJobFlowsResult descResult = emr_.describeJobFlows(desc);
	            for (JobFlowDetail detail : descResult.getJobFlows())
	            {
	                String state = detail.getExecutionStatusDetail().getState();
	                if (isDone(state))
	                {
	                    System.out.println("Job " + state + ": " + detail.toString());
	                    break STATUS_LOOP;
	                }
	                else if (!lastState.equals(state))
	                {
	                    lastState = state;
	                    System.out.println("Job " + state + " at " + new Date().toString());
	                }
	            }
	            Thread.sleep(10000);
	        }
	        
		} catch (Exception e) {
			System.out.print(e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	public void terminateCluster(String clusterId) {
		
		System.out.println("Terminating cluster...");
		
		TerminateJobFlowsRequest terminate = new TerminateJobFlowsRequest()
		.withJobFlowIds(clusterId);
		
		emr_.terminateJobFlows(terminate);
	}
		
    public static boolean isDone(String value)
    {
        JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
        return DONE_STATES.contains(state);
    }
	
}
