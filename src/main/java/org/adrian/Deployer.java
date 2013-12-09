package org.adrian;

import java.io.File;
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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;

public class Deployer {

    private static final String id = new Date().toString();
    
    private static final String s3_jar_file = "s3n://poobar/wordcount.jar";
    private static final String distcp_jar_file = "/home/hadoop/lib/emr-s3distcp-1.0.jar";
    private static final String s3_log_folder = "s3n://poobar/logs";
    
    private static final String jobFlowName = "WordCount-job-flow-" + id;
    
    private static final String mr_main_class = "org.adrian.WordCount";

    private static final String hdfs_input = "hfds://root/adrian";
    private static final String hdfs_output = "hfds://root/adrian/outputs";
    //private static final String mr_cmdline_arg1 = "s3n://poobar/shak2.txt"; //input
    private static final String s3_input = "s3n://poobar/shak2.txt"; //input
    private static final String s3_output = "s3://poobar/outputs/"+id + "/";  //output
    
    public static void main(String[] args) {

        AWSCredentials credentials = new BasicAWSCredentials("AKIAJX23WFZA5737VMVQ", "3FhWX0tK8jWV7Lxi70OKy5R7AAF2VwxA3WTj7sf0");
        ClientConfiguration config = new ClientConfiguration();
                                            //.withProxyHost("surf-proxy.intranet.db.com")
                                            //.withProxyPort(8080);
        
        AmazonElasticMapReduce service = new AmazonElasticMapReduceClient(credentials, config);        
        
        RunJobFlowResult result = service.runJobFlow(jobFlowRequest());
        
        waitForResult(service, result);
        
        downloadResult(credentials, config);
    }


    private static void downloadResult(AWSCredentials credentials, ClientConfiguration config) {
        String outputFile = "outputs/"+id+"/part-r-00000";
        String local = "target/results.out";

        AmazonS3 conn = new AmazonS3Client(credentials, config);
        conn.getObject(
                new GetObjectRequest("poobar", outputFile),
                new File(local)
        );
        
        System.out.println("Downloaded" + outputFile + " to " + local);
    }


    private static DescribeJobFlowsRequest describeResult(RunJobFlowResult result) {
        return new DescribeJobFlowsRequest(Arrays.asList(new String[] { result.getJobFlowId() }));
    }
    
    public static boolean isDone(String value) {
        JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
        return DONE_STATES.contains(state);
    }

    private static List<StepConfig> steps() {
        List<StepConfig> steps = new LinkedList<StepConfig>();
        //steps.add(uploadStep());
        steps.add(mapReduceStep());
        //steps.add(downloadStep());        
        return steps;
    }

    private static StepConfig downloadStep() {
        return new StepConfig()
            .withName("DistCpDownload")
            .withActionOnFailure("TERMINATE_JOB_FLOW")
            .withHadoopJarStep(new HadoopJarStepConfig(distcp_jar_file)
            .withMainClass("com.amazon.elasticmapreduce.s3distcp.Main")
            .withArgs("--src", hdfs_output, "--dest", s3_output));
    }

    private static StepConfig mapReduceStep() {
        return  new StepConfig()
            .withActionOnFailure("TERMINATE_JOB_FLOW")
            .withHadoopJarStep(hadoopJarStepConfig(s3_jar_file, argumentsAsList(s3_input, s3_output), mr_main_class))
            .withName("MRStep" + System.currentTimeMillis());
    }

    private static StepConfig uploadStep() {
        return  new StepConfig()
            .withName("DistCpUpload")
            .withActionOnFailure("TERMINATE_JOB_FLOW")
            .withHadoopJarStep(new HadoopJarStepConfig(distcp_jar_file)
            .withMainClass("com.amazon.elasticmapreduce.s3distcp.Main")
            .withArgs("--src", s3_input, "--dest", hdfs_input));
        
    }

    private static HadoopJarStepConfig hadoopJarStepConfig(String jar, List<String> arguments, String mainClass) {
        return new HadoopJarStepConfig()
            .withArgs(arguments)
            .withJar(jar)
            .withMainClass(mainClass);
    }
    
    private static HadoopJarStepConfig hadoopJarStepConfig(String jar, List<String> arguments) {
        return new HadoopJarStepConfig()
            .withArgs(arguments)
            .withJar(jar);
    }

    private static List<String> argumentsAsList(final String... args) {
        List<String> argList = new LinkedList<String>();
        for (String arg : args){
            argList.add(arg);
        }        
        return argList;
    }

    private static RunJobFlowRequest jobFlowRequest() {
        
        return new RunJobFlowRequest()
            .withName(jobFlowName)            
            //.withBootstrapActions(scriptAction("s3://poobar/install-spark-shark.sh", null))
            .withInstances(jobFlowInstances())
            .withLogUri(s3_log_folder)
            .withSteps(steps());
    }

//    private static BootstrapActionConfig jobTrackerHeapSizeAction(int heap) {
//        BootstrapActions bootstrapActions = new BootstrapActions();
//        return bootstrapActions.newRunIf("instance.isMaster=true", bootstrapActions.newConfigureDaemons()
//                .withHeapSize(Daemon.JobTracker, heap)
//                .build());
//    }
//
//    private static BootstrapActionConfig scriptAction(String bootstrapPath, List<String> args) {
//        BootstrapActionConfig bootstrapConfig = new BootstrapActionConfig();
//        ScriptBootstrapActionConfig bootstrapScriptConfig = new ScriptBootstrapActionConfig();
//        
//        bootstrapScriptConfig.setPath(bootstrapPath);
//
//        if (args != null) {
//            bootstrapScriptConfig.setArgs(args);
//        }
//
//        bootstrapConfig.setName("Custom script action");
//        bootstrapConfig.setScriptBootstrapAction(bootstrapScriptConfig);
//
//        return bootstrapConfig;
//    }   
    
    
    private static JobFlowInstancesConfig jobFlowInstances() {
        JobFlowInstancesConfig conf = new JobFlowInstancesConfig()
            .withInstanceCount(13)
            .withHadoopVersion("0.20.205")
            .withEc2KeyName("adrian")            
            .withKeepJobFlowAliveWhenNoSteps(false)
            .withMasterInstanceType("m1.xlarge")
            .withPlacement(new PlacementType("us-east-1a"))
            .withSlaveInstanceType("m1.xlarge");
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
        
        DONE: while(true) {
            DescribeJobFlowsResult descResult = service.describeJobFlows(describeResult(result));
            
            for (JobFlowDetail detail : descResult.getJobFlows()) {
                String state = detail.getExecutionStatusDetail().getState();
                
                if (isDone(state)) {
                    System.out.println("Job " + state + ": " + detail.toString());
                    System.out.println("Output folder: " + s3_output);
                    break DONE;
                }
                else if (!lastState.equals(state)) {
                    lastState = state;
                    System.out.println("Job " + state + " at " + new Date().toString());
                }
            }
            
            pause();
        } 
    }
    
    private static final List<JobFlowExecutionState> DONE_STATES = Arrays
            .asList(new JobFlowExecutionState[] { JobFlowExecutionState.COMPLETED,
                                                 JobFlowExecutionState.FAILED,
                                                 JobFlowExecutionState.TERMINATED });
        
    
}
