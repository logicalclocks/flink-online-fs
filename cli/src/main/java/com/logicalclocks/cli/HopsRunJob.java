package com.logicalclocks.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hops.cli.action.FileUploadAction;
import io.hops.cli.action.JobRunAction;
import io.hops.cli.config.HopsworksAPIConfig;

import org.apache.http.HttpStatus;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HopsRunJob {

  public void actionPerformed(String jobType, String jarPath) throws Exception {

    // jobs config
    Map<Object, Object> flinkJobsConfig;
    InputStream finkJobConfigStream = getClass().getClassLoader().getResourceAsStream("json/flink_job_config.json");
    try {
      flinkJobsConfig =
          new ObjectMapper().readValue(finkJobConfigStream, HashMap.class);
      finkJobConfigStream.close();
    } catch (Exception e) {
      throw new Exception(e.toString());
    }

    // aggregations config
    Map<Object, Object> aggregationSpecs;
    InputStream aggConfigStream = getClass().getClassLoader().getResourceAsStream("json/flink_aggregations_config.json");
    try {
      aggregationSpecs =
          new ObjectMapper().readValue(aggConfigStream, HashMap.class);
      aggConfigStream.close();
    } catch (Exception e) {
      throw new Exception(e.toString());
    }

    Map<Object, Object> sparkJobConfig;
    InputStream sparkJobConfigStream = getClass().getClassLoader().getResourceAsStream("json/spark_job_config.json");
    try {
      sparkJobConfig =
          new ObjectMapper().readValue(sparkJobConfigStream, HashMap.class);
      sparkJobConfigStream.close();
    } catch (Exception e) {
      throw new Exception(e.toString());
    }

    String hopsworksApiKey = (String) flinkJobsConfig.get("hopsworksApiKey");
    String hopsworksUrl = (String) flinkJobsConfig.get("hopsworksUrl");
    String projectName = (String) flinkJobsConfig.get("projectName");

    String flinkJobName = (String) flinkJobsConfig.get("jobName");
    String flinkDestination = (String) flinkJobsConfig.get("destination");
    String flinkUserArgs = (String) flinkJobsConfig.get("userArgs");
    String flinkMainClass = (String) flinkJobsConfig.get("mainClass");

    String hopsProject=null;
    Integer userExecutionId= (Integer) flinkJobsConfig.get("userExecutionId");;

    int executionId=0;
    if(!userExecutionId.equals(0)){
      try{
        executionId=userExecutionId;
      } catch (NumberFormatException ex){
        System.out.println("Not a valid number execution id; Skipped");
      }
    }

    try {
      int status;
      JobRunAction runJob;
      HopsworksAPIConfig hopsworksAPIConfig = new HopsworksAPIConfig(hopsworksApiKey, hopsworksUrl, projectName);
      FileUploadAction uploadAction = new FileUploadAction(hopsworksAPIConfig, flinkDestination, jarPath);
      hopsProject = uploadAction.getProjectId(); //check if valid project,throws null pointer
      if (!jobType.equals("FLINK")){
        uploadAction.execute(); //upload app first if not flink
        String jobName;
        Integer minSyncIntervalSeconds = (Integer) sparkJobConfig.get("minSyncIntervalSeconds");
        String sparkJobArgs;
        String sparkMainClass = (String) sparkJobConfig.get("sparkMainClass");
        if (sparkMainClass.equals("com.logicalclocks.hudi.DeltaStreamerJob")) {
          jobName = "deltaStreamerJob";
          sparkJobArgs = String.format("-featureGroupName %s -featureGroupVersion %d -minSyncIntervalSeconds %d",
              (String) aggregationSpecs.get("feature_group_name"),
              (Integer) aggregationSpecs.get("feature_group_version"), minSyncIntervalSeconds);
        } else {
          jobName = "microbatching";
          sparkJobArgs = String.format("-featureGroupName %s -featureGroupVersion %d",
              (String) aggregationSpecs.get("feature_group_name"),
              (Integer) aggregationSpecs.get("feature_group_version"));
        }

        runJob=new JobRunAction(hopsworksAPIConfig,jobName,sparkJobArgs);
        if(!runJob.getJobExists()){ //check job name exists
          System.out.println("e.getProject(), HopsPluginUtils.INVALID_JOBNAME" + sparkJobArgs);
          return;
        }
        //execute run job
        status=runJob.execute();
        if (status == HttpStatus.SC_OK || status == HttpStatus.SC_CREATED) {
          StringBuilder sb=new StringBuilder(" Job Submitted: ").append(jobName)
              .append(" | Execution Id: ").append(runJob.getJsonResult().getInt("id"));
          System.out.println("e.getProject()" + sb.toString());
        } else {
          if(runJob.getJsonResult().containsKey("usrMsg"))
            System.out.println("e.getProject()" + " Job Submit Failed | "
                +runJob.getJsonResult().getString("usrMsg"));
          else System.out.println("e.getProject()" + " Job: "+flinkJobName+" | Submit Failed");
        }
      } else {
        SubmitFlinkJob submitFlinkJob = new SubmitFlinkJob(hopsworksAPIConfig, flinkJobName);
        submitFlinkJob.setLocal_file_path(jarPath);
        submitFlinkJob.setMainClass(flinkMainClass);
        submitFlinkJob.setUserArgs(flinkUserArgs);
        submitFlinkJob.setUserExecId(executionId);
        status=submitFlinkJob.execute();
        if (status== HttpStatus.SC_OK){
          System.out.println("e.getProject()" +
              "Flink job was submitted successfully, please check Hopsworks UI for progress.");
        }
      }
    } catch (IOException ex) {
      System.out.println("e.getProject()" + ex.getMessage());
      Logger.getLogger(JobRunAction.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
    } catch (NullPointerException nullPointerException) {
      if (hopsProject == null) {
        System.out.println("e.getProject(), HopsPluginUtils.INVALID_PROJECT");
      } else {
        System.out.println("e.getProject()" + nullPointerException.toString());
      }
      Logger.getLogger(HopsCreateJob.class.getName()).log(Level.SEVERE, nullPointerException.toString(), nullPointerException);
    } catch (Exception ex) {
      System.out.println("e.getProject()" + ex.getMessage());
      Logger.getLogger(HopsRunJob.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("jobType", "jobType", true, "Job type SPARK|FLINK");
    options.addOption("jarFilePath", "jarFilePath", true,
        "path to spark program binary");

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);

    HopsRunJob hopsRunJob = new HopsRunJob();
    hopsRunJob.actionPerformed(commandLine.getOptionValue("jobType"),
        commandLine.getOptionValue("jarFilePath"));
  }
}
