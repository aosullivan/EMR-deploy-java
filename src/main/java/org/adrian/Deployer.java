package org.adrian;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
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

    public static void main(String[] args) {

        AWSCredentials credentials = new BasicAWSCredentials("AKIAJX23WFZA5737VMVQ", "3FhWX0tK8jWV7Lxi70OKy5R7AAF2VwxA3WTj7sf0");
        AmazonElasticMapReduce service = new AmazonElasticMapReduceClient(credentials);
        
        RunJobFlowRequest request = jobFlowRequest();
        RunJobFlowResult result = service.runJobFlow(request);
        
        result.toString();

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
        conf.setEc2KeyName("class");
        conf.setInstanceCount(11);
        conf.setKeepJobFlowAliveWhenNoSteps(true);
        conf.setMasterInstanceType("m1.small");
        conf.setPlacement(new PlacementType("us-east-1"));
        conf.setSlaveInstanceType("m1.small");
        return conf;
    }

}
