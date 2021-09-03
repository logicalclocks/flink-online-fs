/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.logicalclocks.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hops.cli.action.FileUploadAction;
import io.hops.cli.action.JobCreateAction;
import io.hops.cli.config.HopsworksAPIConfig;
import org.apache.http.HttpStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HopsCreateJob {

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
    Integer jobManagerMemory  = (Integer) jobsConfig.get("jobManagerMemory");;
    Integer taskManagerMemory =  (Integer) jobsConfig.get("taskManagerMemory");;
    Integer numTaskManager = (Integer) jobsConfig.get("numTaskManager");;
    Integer numSlots = (Integer) jobsConfig.get("numSlots");;
    Boolean isAdvanced = (Boolean) jobsConfig.get("isAdvanced");;
    String advancedProperties = (String) jobsConfig.get("advancedProperties");

    String mainClass = (String) jobsConfig.get("mainClass");
    String localFilePath = "/Users/davitbz/IdeaProjects/flink-online-fs/flink/target/flink-1.0-SNAPSHOT.jar";
    File file = new File(localFilePath);
    String finalPath = destination + File.separator + file.getName();

    String hopsProject = null;
    try {
      HopsworksAPIConfig hopsworksAPIConfig = new HopsworksAPIConfig(hopsworksApiKey, hopsworksUrl, projectName);
      //upload program
      FileUploadAction uploadAction = new FileUploadAction(hopsworksAPIConfig, destination, localFilePath);
      hopsProject = uploadAction.getProjectId(); //check if valid project,throws null pointer
      // upload program if not flink
      if (!jobType.equals("FLINK")) //HopsPluginUtils.FLINK
        uploadAction.execute();
      //set program configs
      JobCreateAction.Args args = new JobCreateAction.Args();
      args.setMainClass(mainClass); //set user provides,overridden by inspect job config
      args.setAppPath(finalPath); //full app path
      args.setJobType(jobType);  // spark/flink
      args.setCommandArgs(userArgs.trim());
      switch (jobType) {
        case "SPARK": //HopsPluginUtils.SPARK
          args.setDriverMemInMbs(2048); //Integer.parseInt(util.getDriverMemory(proj))
          args.setDriverVC(1); //Integer.parseInt(util.getDriverVC(proj))
          args.setExecutorMemInMbs(4096); //Integer.parseInt(util.getExecutorMemory(proj))
          args.setExecutorVC(1); //"Integer.parseInt(util.getExecutorVC(proj))";
          args.setDynamic(true); //"HopsPluginUtils.isSparkDynamic(proj)"
          if (false) //!HopsPluginUtils.isSparkDynamic(proj)
            args.setNumExecutors(1); //Integer.parseInt(util.getNumberExecutor(proj))
          else {
            args.setInitExecutors(1); //Integer.parseInt(HopsPluginUtils.getInitExec(proj))
            args.setMaxExecutors(2); //Integer.parseInt(HopsPluginUtils.getMaxExec(proj))
            args.setMinExecutors(1); //Integer.parseInt(HopsPluginUtils.getMinExec(proj))
          }
          if (false) { //util.isAdvanced(proj)
            args.setAdvanceConfig(true);
            args.setArchives(""); //util.getAdvancedArchive(proj)
            args.setFiles(""); //util.getAdvancedFiles(proj)
            args.setPythonDependency(""); //util.getPythonDependency(proj)
            args.setJars(""); //util.getAdvancedJars(proj)
            args.setProperties(""); //util.getMoreProperties(proj)
          }
          break;
        case "FLINK":
          args.setJobManagerMemory(jobManagerMemory);
          args.setTaskManagerMemory(taskManagerMemory);
          args.setNumTaskManager(numTaskManager);
          args.setNumSlots(numSlots);
          if (isAdvanced) {
            args.setAdvanceConfig(true);
            args.setProperties(advancedProperties);
          }
          break;
      }

      // create job
      JobCreateAction createJob = new JobCreateAction(hopsworksAPIConfig, jobName, args);
      int status = createJob.execute();

      if (status == HttpStatus.SC_OK || status == HttpStatus.SC_CREATED) {
        System.out.println("Job Created: " + jobName);
      }else {
        if(createJob.getJsonResult().containsKey("usrMsg"))
          System.out.println(" Job Create Failed | "+createJob.getJsonResult().getString("usrMsg"));

        else System.out.println("Job Creation Failed: " + jobName);
      }

    } catch (IOException ioException) {
      System.out.println(ioException.getMessage());
      Logger.getLogger(JobCreateAction.class.getName()).log(Level.SEVERE, ioException.getMessage(), ioException);
    } catch (NullPointerException nullPointerException) {
      if (hopsProject == null) {
        System.out.println( "INVALID_PROJECT");
        Logger.getLogger(HopsCreateJob.class.getName()).log(Level.SEVERE, nullPointerException.toString(), nullPointerException);
      } else {
        System.out.println(nullPointerException.toString());
      }
      Logger.getLogger(HopsCreateJob.class.getName()).log(Level.SEVERE, nullPointerException.toString(), nullPointerException);
    } catch (Exception exception) {
      System.out.println(exception.toString());
      Logger.getLogger(HopsCreateJob.class.getName()).log(Level.SEVERE, exception.getMessage(), exception);
    }
  }

  public static void main(String[] args) throws Exception {
    HopsCreateJob hopsCreateJob = new HopsCreateJob();
    hopsCreateJob.actionPerformed();
  }
}
