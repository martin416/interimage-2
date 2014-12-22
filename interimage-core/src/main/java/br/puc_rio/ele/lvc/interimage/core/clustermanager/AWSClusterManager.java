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
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

/**
 * Implementation of the cluster manager for Amazon. 
 * @author Rodrigo Ferreira
 */

public class AWSClusterManager implements ClusterManager {

	private AmazonElasticMapReduce emr_;
	private Source source_;
	private Properties properties_;
	
	/*private static final List<JobFlowExecutionState> JOB_DONE_STATES = Arrays
	        .asList(new JobFlowExecutionState[] { JobFlowExecutionState.COMPLETED,
	                                             JobFlowExecutionState.FAILED,
	                                             JobFlowExecutionState.TERMINATED});*/
		
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
		String logging = properties_.getProperty("interimage.aws.logging");
		String instanceType = properties_.getProperty("interimage.aws.instanceType");
		String market = properties_.getProperty("interimage.aws.market");
		String bidPrice = properties_.getProperty("interimage.aws.bidPrice");
		String hadoopVersion = properties_.getProperty("interimage.aws.hadoopVersion");
		String region = properties_.getProperty("interimage.aws.region");
		
		int cores = Integer.parseInt(properties_.getProperty("interimage.cores"));
		//long memory = Long.parseLong(properties_.getProperty("interimage.memory"));
		int clusterSize = Integer.parseInt(properties_.getProperty("interimage.clusterSize"));
		
		StepFactory stepFactory = new StepFactory();
		
		StepConfig enableDebugging = null;
		
		BootstrapActions bootstrapActions = new BootstrapActions();
		
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
		.withInstanceCount(clusterSize-1)
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

		//int mapSlotsPerTasktracker = (int)Math.round(cores*1.5*(2.0/3.0));
		int mapSlotsPerTasktracker = (int)Math.round(cores*2);
		//int reduceSlotsPerTasktracker = (int)Math.round(cores*1.5*(1.0/3.0));
		int reduceSlotsPerTasktracker = (int)Math.round(cores*2);
		int heapSizeMBytes = 1500;
		long heapSizeKBytes = heapSizeMBytes * 1024;
		//long heapSizeBytes = heapSizeGBytes * 1024 * 1024 * 1024;
		
		//Bootstrap actions
		List<BootstrapActionConfig> bootstrapList = Arrays.asList(
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapreduce.job.counters.limit", "1200").build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.task.timeout", "1800000").build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.reduce.tasks.speculative.execution", "false").build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.map.tasks.speculative.execution", "false").build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.tasktracker.map.tasks.maximum", String.valueOf(mapSlotsPerTasktracker)).build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.tasktracker.reduce.tasks.maximum", String.valueOf(reduceSlotsPerTasktracker)).build(),
				//bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.reduce.slowstart.completed.maps", "0.8").build(),
				//bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "tasktracker.http.threads", "64").build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.reduce.tasks", String.valueOf((int)Math.round((clusterSize-1)*reduceSlotsPerTasktracker))).build(),
				//bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.reduce.parallel.copies", String.valueOf((int)Math.round(Math.log(clusterSize-1)*4))).build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.jobtracker.taskScheduler", "org.apache.hadoop.mapred.FairScheduler").build(),
				//bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.job.tracker.handler.count", String.valueOf((int)Math.round(Math.log(clusterSize-1)*20))).build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.output.compression.type", "BLOCK").build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec").build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.compress.map.output", "true").build(),
				//bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "io.sort.factor", "64").build(),
				//bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "io.sort.mb", "128").build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.child.ulimit", String.valueOf((int)heapSizeKBytes*1.5)).build(),
				bootstrapActions.newConfigureHadoop().withKeyValue(BootstrapActions.ConfigFile.Mapred, "mapred.java.child.opts", "-Xmx" + String.valueOf(heapSizeMBytes) + "m").build()
				);
		
				
		RunJobFlowRequest request = new RunJobFlowRequest()
	    .withName("Pig Interactive")
	    //.withAmiVersion("3.1.0")
	    //.withVisibleToAllUsers(true)
	    .withInstances(instances)
	    .withBootstrapActions(bootstrapList);
		
		if (logging.equals("true"))
			request.setLogUri("s3://" + properties_.getProperty("interimage.aws.S3Bucket") + "/interimage/" + properties_.getProperty("interimage.projectName"));
		
		if (debugging.equals("true")) {
			request.setSteps(Arrays.asList(enableDebugging, installPig));
		} else {
			request.setSteps(Arrays.asList(installPig));
		}
		
		RunJobFlowResult result = emr_.runJobFlow(request);
		
		return result.getJobFlowId();
		
	}
	
	public ExecutionTracker runPigScript(String script, String clusterId) {
		
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
		
		AddJobFlowStepsResult result = emr_.addJobFlowSteps(new AddJobFlowStepsRequest().withJobFlowId(clusterId).withSteps(runPigScript));
		
		AWSExecutionTracker tracker = new AWSExecutionTracker();
		tracker.setJobFlowStepsResult(result);
		tracker.setClusterId(clusterId);
		tracker.setAmazonElasticMapReduce(emr_);
		
		return tracker;
		
		/*try {
		
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
		}*/
		
	}
	
	public void terminateCluster(String clusterId) {
		
		System.out.println("Terminating cluster...");
		
		TerminateJobFlowsRequest terminate = new TerminateJobFlowsRequest()
		.withJobFlowIds(clusterId);
		
		emr_.terminateJobFlows(terminate);
	}
		
    /*public static boolean jobIsDone(String value)
    {
        JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
        return JOB_DONE_STATES.contains(state);
    }*/
    
}
