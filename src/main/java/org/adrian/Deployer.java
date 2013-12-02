package org.adrian;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowExecutionState;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class Deployer {
    
    private static final String s3_jar_file = "s3n://poobar/wordcount.jar";
    private static final String s3_log_folder = "s3n://poobar/logs";
    
    private static final String jobFlowName = "Class-job-flow" + new Date().toString();
    private static final String stepname = "Step" + System.currentTimeMillis();
    
    private static final String mr_main_class = "org.adrian.WordCount";
    private static final String mr_cmdline_arg1 = "s3n://poobar/hamlet111.txt";
    private static final String mr_cmdline_arg2 = "s3n://poobar/outputs/"+jobFlowName+"/"+stepname+"/";
    
    private static final List<JobFlowExecutionState> DONE_STATES = Arrays
            .asList(new JobFlowExecutionState[] { JobFlowExecutionState.COMPLETED,
                                                 JobFlowExecutionState.FAILED,
                                                 JobFlowExecutionState.TERMINATED });
        
    public static void main(String[] args) {

        AWSCredentials credentials = new BasicAWSCredentials("AKIAJX23WFZA5737VMVQ", "3FhWX0tK8jWV7Lxi70OKy5R7AAF2VwxA3WTj7sf0");
        ClientConfiguration config = new ClientConfiguration()
                                            .withProxyHost("surf-proxy.intranet.db.com")
                                            .withProxyPort(8080);
        AmazonElasticMapReduce service = new AmazonElasticMapReduceClient(credentials, config);
        
        RunJobFlowRequest request = jobFlowRequest();
        RunJobFlowResult result = service.runJobFlow(request);
        
        waitForResult(service, result);
        
    }


    private static DescribeJobFlowsRequest describeResult(RunJobFlowResult result) {
        return new DescribeJobFlowsRequest(Arrays.asList(new String[] { result.getJobFlowId() }));
    }
    
    public static boolean isDone(String value)
    {
        JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
        return DONE_STATES.contains(state);
    }

    private static StepConfig stepConfig(final String stepname, HadoopJarStepConfig jarsetup) {
        StepConfig stepConfig = new StepConfig();
        stepConfig.setActionOnFailure("CANCEL_AND_WAIT");
        stepConfig.setHadoopJarStep(jarsetup);
        stepConfig.setName(stepname);
        return stepConfig;
    }

    private static HadoopJarStepConfig hadoopJarSetupConfig(List<String> arguments) {
        HadoopJarStepConfig jarsetup = new HadoopJarStepConfig();
        jarsetup.setArgs(arguments);
        jarsetup.setJar(s3_jar_file);
        jarsetup.setMainClass(mr_main_class);
        return jarsetup;
    }

    private static List<String> argumentsAsList(final String jobFlowName, final String stepname) {
        List<String> arguments = new LinkedList<String>();
        arguments.add(mr_cmdline_arg1);
        arguments.add(mr_cmdline_arg2);
        return arguments;
    }

    private static List<StepConfig> asList(StepConfig stepConfig) {
        List<StepConfig> steps = new LinkedList<StepConfig>();
        steps.add(stepConfig);
        return steps;
    }

    private static RunJobFlowRequest jobFlowRequest() {
        
        HadoopJarStepConfig jar = hadoopJarSetupConfig(argumentsAsList(jobFlowName, stepname));        
        RunJobFlowRequest request = new RunJobFlowRequest();
        request.setInstances(jobFlowInstance());
        request.setLogUri(s3_log_folder);
        request.setName(jobFlowName);
        request.setSteps(asList(stepConfig(stepname, jar)));
        
        return request;
    }

    private static JobFlowInstancesConfig jobFlowInstance() {
        JobFlowInstancesConfig conf = new JobFlowInstancesConfig();
        //conf.setEc2KeyName("class");
        conf.setInstanceCount(5);
        conf.setKeepJobFlowAliveWhenNoSteps(true);
        conf.setMasterInstanceType("m1.small");
        conf.setPlacement(new PlacementType("us-east-1a"));
        conf.setSlaveInstanceType("m1.small");
        return conf;
    }
    
    private static void pause() {
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void waitForResult(AmazonElasticMapReduce service, RunJobFlowResult result) {
        //Check the status of the running job
        String lastState = "";
        STATUS_LOOP: while (true) {
            DescribeJobFlowsResult descResult = service.describeJobFlows(describeResult(result));
            
            for (JobFlowDetail detail : descResult.getJobFlows()) {
                String state = detail.getExecutionStatusDetail().getState();
                
                if (isDone(state)) {
                    System.out.println("Job " + state + ": " + detail.toString());
                    break STATUS_LOOP;
                }
                else if (!lastState.equals(state)) {
                    lastState = state;
                    System.out.println("Job " + state + " at " + new Date().toString());
                }
            }
            
            pause();
        }
    }
    
}
