package com.logicalclocks.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hops.cli.action.FileUploadAction;
import io.hops.cli.action.JobRunAction;
import io.hops.cli.config.HopsworksAPIConfig;
import org.apache.http.HttpStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HopsRunJob {

  public void actionPerformed() throws Exception {

    // jobs config
    Map<Object, Object> jobsConfig;
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("json/flink_job_config.json");
    try {
      jobsConfig =
          new ObjectMapper().readValue(inputStream, HashMap.class);
      inputStream.close();
    } catch (Exception e) {
      throw new Exception(e.toString());
    }

    String hopsworksApiKey = (String) jobsConfig.get("hopsworksApiKey");
    String hopsworksUrl = (String) jobsConfig.get("hopsworksUrl");
    String projectName = (String) jobsConfig.get("projectName");
    String jobName = (String) jobsConfig.get("jobName");
    String destination = (String) jobsConfig.get("destination");
    String userArgs = (String) jobsConfig.get("userArgs");

    String jobType = (String) jobsConfig.get("jobType");

    String mainClass = (String) jobsConfig.get("mainClass");
    String localFilePath = "/Users/davitbz/IdeaProjects/flink-online-fs/flink/target/flink-1.0-SNAPSHOT.jar";
    File file = new File(localFilePath);
    String finalPath = destination + File.separator + file.getName();

    String hopsProject=null;
    Integer userExecutionId= (Integer) jobsConfig.get("userExecutionId");;

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
      FileUploadAction uploadAction = new FileUploadAction(hopsworksAPIConfig,destination,localFilePath);
      hopsProject = uploadAction.getProjectId(); //check if valid project,throws null pointer
      if (!jobType.equals("FLINK")){ //HopsPluginUtils.FLINK
        uploadAction.execute(); //upload app first if not flink
        runJob=new JobRunAction(hopsworksAPIConfig,jobName,userArgs);
        if(!runJob.getJobExists()){ //check job name exists
          System.out.println("e.getProject(), HopsPluginUtils.INVALID_JOBNAME" + jobName);
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
          else System.out.println("e.getProject()" + " Job: "+jobName+" | Submit Failed");
        }
      } else {
        SubmitFlinkJob submitFlinkJob = new SubmitFlinkJob(hopsworksAPIConfig, jobName);
        submitFlinkJob.setLocal_file_path(localFilePath);
        submitFlinkJob.setMainClass(mainClass);
        submitFlinkJob.setUserArgs(userArgs);
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
    HopsRunJob hopsRunJob = new HopsRunJob();
    hopsRunJob.actionPerformed();
  }
}
