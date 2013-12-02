package org.adrian;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
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
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions.Daemon;

public class Deployer {
    
    private static final String s3_jar_file = "s3n://poobar/wordcount.jar";
    private static final String s3_log_folder = "s3n://poobar/logs";
    
    private static final String jobFlowName = "WordCount-job-flow-" + new Date().toString();
    private static final String stepname = "Step" + System.currentTimeMillis();
    
    private static final String mr_main_class = "org.adrian.WordCount";
    private static final String mr_cmdline_arg1 = "s3n://poobar/hamlet111.txt";
    private static final UUID RANDOM_UUID = UUID.randomUUID();
    private static final String mr_cmdline_arg2 = "poobar/outputs/"+RANDOM_UUID.toString() + "/";
    
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
    
    public static boolean isDone(String value) {
        JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
        return DONE_STATES.contains(state);
    }

    private static StepConfig stepConfig(final String stepname) {
        return new StepConfig()
            .withActionOnFailure("TERMINATE_JOB_FLOW")
            .withHadoopJarStep(hadoopJarStepConfig(argumentsAsList(jobFlowName, stepname)))
            .withName(stepname);
    }

    private static HadoopJarStepConfig hadoopJarStepConfig(List<String> arguments) {
        return new HadoopJarStepConfig()
            .withArgs(arguments)
            .withJar(s3_jar_file)
            .withMainClass(mr_main_class);
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
        BootstrapActions bootstrapActions = new BootstrapActions();
        return new RunJobFlowRequest()
            .withName(jobFlowName)
            .withBootstrapActions(bootstrapActions.newRunIf(
               "instance.isMaster=true", action(bootstrapActions)))
            .withInstances(jobFlowInstance())
            .withLogUri(s3_log_folder)
            .withSteps(asList(stepConfig(stepname)));
    }


    private static BootstrapActionConfig action(BootstrapActions bootstrapActions) {
        return bootstrapActions.newConfigureDaemons()
               .withHeapSize(Daemon.JobTracker, 2048)
               .build();
    }

    private static JobFlowInstancesConfig jobFlowInstance() {
        JobFlowInstancesConfig conf = new JobFlowInstancesConfig()
            .withInstanceCount(3)
            .withHadoopVersion("0.20.205")
            .withKeepJobFlowAliveWhenNoSteps(true)
            .withMasterInstanceType("m1.small")
            .withPlacement(new PlacementType("us-east-1a"))
            .withSlaveInstanceType("m1.small");
        return conf;
    }
    
    private static void pause() {
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void waitForResult(AmazonElasticMapReduce service, RunJobFlowResult result) {
        String lastState = "";
        
        do {
            DescribeJobFlowsResult descResult = service.describeJobFlows(describeResult(result));
            
            for (JobFlowDetail detail : descResult.getJobFlows()) {
                String state = detail.getExecutionStatusDetail().getState();
                
                if (isDone(state)) {
                    System.out.println("Job " + state + ": " + detail.toString());
                    break;
                }
                else if (!lastState.equals(state)) {
                    lastState = state;
                    System.out.println("Job " + state + " at " + new Date().toString());
                }
            }
            
            pause();
        } while (true);
    }
    
}
