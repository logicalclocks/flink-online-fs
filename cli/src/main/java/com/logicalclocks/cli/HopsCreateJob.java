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

import io.hops.cli.config.HopsworksAPIConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpStatus;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HopsCreateJob {

  public void actionPerformed(String jobType, String sparkJarFilePath) throws Exception {

    // jobs config
    Map<Object, Object> flinkJobConfig;
    InputStream finkJobConfigStream = getClass().getClassLoader().getResourceAsStream("json/flink_job_config.json");
    try {
      flinkJobConfig =
          new ObjectMapper().readValue(finkJobConfigStream, HashMap.class);
      finkJobConfigStream.close();
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

    String hopsworksApiKey = (String) flinkJobConfig.get("hopsworksApiKey");
    String hopsworksUrl = (String) flinkJobConfig.get("hopsworksUrl");
    String projectName = (String) flinkJobConfig.get("projectName");
    String destination = "/Projects/" + projectName + "/Resources";

    Integer flinkJobManagerMemory  = (Integer) flinkJobConfig.get("jobManagerMemory");;
    Integer flinkTaskManagerMemory =  (Integer) flinkJobConfig.get("taskManagerMemory");;
    Integer flinkNumTaskManager = (Integer) flinkJobConfig.get("numTaskManager");;
    Integer flinkNumSlots = (Integer) flinkJobConfig.get("numSlots");;
    Boolean flinkIsAdvanced = (Boolean) flinkJobConfig.get("isAdvanced");;
    String flinkAdvancedProperties = (String) flinkJobConfig.get("advancedProperties");

    Integer sparkDriverMemInMbs = (Integer) sparkJobConfig.get("driverMemInMbs");
    Integer sparkDriverVC = (Integer) sparkJobConfig.get("driverVC");
    Integer sparkExecutorMemInMbs = (Integer) sparkJobConfig.get("executorMemInMbs");
    Integer sparkExecutorVC = (Integer) sparkJobConfig.get("executorVC");
    boolean sparkDynamic =(boolean) sparkJobConfig.get("dynamic");
    Integer sparkNumExecutors = (Integer) sparkJobConfig.get("numExecutors");
    Integer sparkInitExecutors = (Integer) sparkJobConfig.get("initExecutors");
    Integer sparkMaxExecutors = (Integer) sparkJobConfig.get("maxExecutors");
    Integer sparkMinExecutors = (Integer) sparkJobConfig.get("minExecutors");
    boolean sparkAdvanceConfig = (boolean) sparkJobConfig.get("advanceConfig");
    String sparkArchives = (String) sparkJobConfig.get("archives");
    String sparkAttachfiles = (String) sparkJobConfig.get("files");
    String sparkPythonDependency = (String) sparkJobConfig.get("pythonDependency");
    String sparkAttachjars = (String) sparkJobConfig.get("jars");
    String sparkProperties = (String) sparkJobConfig.get("properties");
    String sparkMainClass = (String) sparkJobConfig.get("sparkMainClass");
    Integer minSyncIntervalSeconds = (Integer) sparkJobConfig.get("minSyncIntervalSeconds");
    String sparkJobArgs;

    String hopsProject = null;
    String jobName = null;
    if (sparkMainClass.equals("com.logicalclocks.hudi.DeltaStreamerJob")) {
      jobName = "deltaStreamerJob";
      sparkJobArgs =  String.format("-featureGroupName %s -featureGroupVersion %d -minSyncIntervalSeconds %d",
          (String) aggregationSpecs.get("feature_group_name"),
          (Integer) aggregationSpecs.get("feature_group_version"),
          minSyncIntervalSeconds);
    } else {
      jobName = "microbatching";
      sparkJobArgs =  String.format("-featureGroupName %s -featureGroupVersion",
          (String) aggregationSpecs.get("feature_group_name"),
          (Integer) aggregationSpecs.get("feature_group_version"));
    }

    try {
      HopsworksAPIConfig hopsworksAPIConfig = new HopsworksAPIConfig(hopsworksApiKey, hopsworksUrl, projectName);
      //upload program
      FileUploadAction uploadAction = new FileUploadAction(hopsworksAPIConfig, destination, sparkJarFilePath);
      hopsProject = uploadAction.getProjectId(); //check if valid project,throws null pointer
      // upload program if not flink
      if (!jobType.equals("FLINK")) //HopsPluginUtils.FLINK
        uploadAction.execute();
      //set program configs
      JobCreateAction.Args args = new JobCreateAction.Args();
      args.setJobType(jobType);  // spark/flink
      switch (jobType) {
        case "SPARK":
          File sparkFile = new File(sparkJarFilePath);
          String sparkJarFinalPath = "hdfs://" + destination + File.separator + sparkFile.getName();
          args.setMainClass(sparkMainClass);
          args.setAppPath(sparkJarFinalPath);
          args.setCommandArgs(sparkJobArgs);

          args.setDriverMemInMbs(sparkDriverMemInMbs);
          args.setDriverVC(sparkDriverVC);
          args.setExecutorMemInMbs(sparkExecutorMemInMbs);
          args.setExecutorVC(sparkExecutorVC);
          args.setDynamic(sparkDynamic);
          if (sparkDynamic)
            args.setNumExecutors(sparkNumExecutors);
          else {
            args.setInitExecutors(sparkInitExecutors);
            args.setMaxExecutors(sparkMaxExecutors);
            args.setMinExecutors(sparkMinExecutors);
          }
          if (sparkAdvanceConfig) {
            args.setAdvanceConfig(sparkAdvanceConfig);
            args.setArchives(sparkArchives);
            args.setFiles(sparkAttachfiles);
            args.setPythonDependency(sparkPythonDependency);
            args.setJars(sparkAttachjars);
            args.setProperties(sparkProperties);
          }
          break;
        case "FLINK":
          jobName = (String) flinkJobConfig.get("jobName");

          args.setJobManagerMemory(flinkJobManagerMemory);
          args.setTaskManagerMemory(flinkTaskManagerMemory);
          args.setNumTaskManager(flinkNumTaskManager);
          args.setNumSlots(flinkNumSlots);
          if (flinkIsAdvanced) {
            args.setAdvanceConfig(true);
            args.setProperties(flinkAdvancedProperties);
          }
          break;
      }

      // create job
      JobCreateAction createJob = createJob = new JobCreateAction(hopsworksAPIConfig, jobName, args);
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
    Options options = new Options();
    options.addOption("jobType", "jobType", true, "Job type SPARK|FLINK");
    options.addOption("sparkJarFilePath", "sparkJarFilePath", true,
        "path to spark program binary");

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);

    HopsCreateJob hopsCreateJob = new HopsCreateJob();
    hopsCreateJob.actionPerformed(commandLine.getOptionValue("jobType"), commandLine.getOptionValue("sparkJarFilePath"));
  }
}
