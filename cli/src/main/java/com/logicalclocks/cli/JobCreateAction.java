package com.logicalclocks.cli;

import io.hops.cli.action.JobAction;
import io.hops.cli.config.HopsworksAPIConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;

import static org.apache.http.HttpHeaders.USER_AGENT;

public class JobCreateAction extends JobAction {

  private static final String SPARK = "SPARK";
  private static final String PYTHON = "PYTHON";
  private static final String FLINK = "FLINK";
  private static final String PYTHON_JOB_CONFIG="pythonJobConfiguration";
  private static final String FLINK_JOB_CONFIG="flinkJobConfiguration";

  @XmlRootElement
  public static class Args {
    private int numExecutors=1;
    private int initExecutors=1;
    private int minExecutors=1;
    private int maxExecutors=100;
    private int cpusPerExecutor=1;
    private int cpusPerDriver=1;
    private int driverMemInMbs = 2048;
    private int executorMemInMbs = 4096;
    private int gpusPerExecutor = 0;
    private String commandArgs = "";
    private String jvmArgs = "";
    private String[] sparkConf = {};
    private String files = "";
    private String jars = "";
    private String archives = "";
    private String properties = "";
    private String pythonDependency = "";
    private String appPath="";
    private String mainClass="";
    private String configType="";
    private String jobType="";
    private int driverVC=1;
    private int executorVC=1;
    private int cpusCores=1;
    private int pythonMemory=2048;
    private int jobManagerMemory=1024;
    private int numTaskManager=1;
    private int taskManagerMemory=1024;
    private int numSlots=1;
    private boolean isAdvanceConfig=false;
    private boolean isDynamicAllocation =false;

    public int getInitExecutors() {
      return initExecutors;
    }

    public void setInitExecutors(int initExecutors) {
      this.initExecutors = initExecutors;
    }

    public int getMinExecutors() {
      return minExecutors;
    }

    public void setMinExecutors(int minExecutors) {
      this.minExecutors = minExecutors;
    }

    public int getMaxExecutors() {
      return maxExecutors;
    }

    public void setMaxExecutors(int maxExecutors) {
      this.maxExecutors = maxExecutors;
    }

    public boolean isDynamic() {
      return isDynamicAllocation;
    }

    public void setDynamic(boolean dynamic) {
      isDynamicAllocation = dynamic;
    }

    public Args() {}

    public int getNumExecutors() {
      return numExecutors;
    }

    public void setNumExecutors(int numExecutors) {
      this.numExecutors = numExecutors;
    }

    public int getCpusPerExecutor() {
      return cpusPerExecutor;
    }

    public void setCpusPerExecutor(int cpusPerExecutor) {
      this.cpusPerExecutor = cpusPerExecutor;
    }

    public int getCpusPerDriver() {
      return cpusPerDriver;
    }

    public void setCpusPerDriver(int cpusPerDriver) {
      this.cpusPerDriver = cpusPerDriver;
    }

    public int getDriverMemInMbs() { return driverMemInMbs; }

    public void setDriverMemInMbs(int driverMemInMbs) {
      this.driverMemInMbs = driverMemInMbs;
    }

    public int getExecutorMemInMbs() {
      return executorMemInMbs;
    }

    public void setExecutorMemInMbs(int executorMemInMbs) {
      this.executorMemInMbs = executorMemInMbs;
    }

    public int getGpusPerExecutor() {
      return gpusPerExecutor;
    }

    public void setGpusPerExecutor(int gpusPerExecutor) {
      this.gpusPerExecutor = gpusPerExecutor;
    }

    public String getCommandArgs() { return commandArgs; }

    public void setCommandArgs(String commandArgs) {
      this.commandArgs = commandArgs;
    }

    public String getJvmArgs() {
      return jvmArgs;
    }

    public void setJvmArgs(String jvmArgs) {
      this.jvmArgs = jvmArgs;
    }

    public String getFiles() {
      return files;
    }

    public void setFiles(String files) {
      this.files = files;
    }

    public String getJars() {
      return jars;
    }

    public void setJars(String jars) {
      this.jars = jars;
    }

    public String getArchives() {
      return archives;
    }

    public void setArchives(String archives) {
      this.archives = archives;
    }

    public String getProperties() {
      return properties;
    }

    public void setProperties(String properties) {
      this.properties = properties;
    }

    public String getPythonDependency() {
      return pythonDependency;
    }

    public void setPythonDependency(String pythonDependency) {
      this.pythonDependency = pythonDependency;
    }

    public String[] getSparkConf() { return sparkConf;  }

    public void setSparkConf(String[] sparkConf) {
      this.sparkConf = sparkConf;
    }

    public int getNumSlots() {
      return numSlots;
    }

    public void setNumSlots(int numSlots) {
      this.numSlots = numSlots;
    }

    public String getAppPath() {
      return appPath;
    }

    public void setAppPath(String s) {
      this.appPath = s;
    }

    public String getMainClass() {
      return mainClass;
    }

    public void setMainClass(String s) {
      this.mainClass = s;
    }

    public String getConfigType() {
      return configType;
    }

    public void setConfigType(String s) {
      this.configType = s;
    }

    public String getJobType() {
      return jobType;
    }

    public void setJobType(String s) {
      this.jobType = s;
    }

    public int getNumTaskManager() {
      return numTaskManager;
    }

    public void getNumTaskManager(int numTaskManager) {
      this.numTaskManager = numTaskManager;
    }

    public int getTaskManagerMemory() {  return taskManagerMemory;   }

    public void setDriverVC(int driverVC) {   this.driverVC = driverVC;  }

    public void setExecutorVC(int executorVC) { this.executorVC = executorVC;  }

    public void setCpusCores(int cpusCores) { this.cpusCores = cpusCores;  }

    public void setPythonMemory(int pythonMemory) { this.pythonMemory = pythonMemory; }

    public void setJobManagerMemory(int jobManagerMemory) { this.jobManagerMemory = jobManagerMemory;
    }

    public void setNumTaskManager(int numTaskManager) {this.numTaskManager = numTaskManager;
    }

    public int getExecutorVC() {  return executorVC;    }

    public int getCpusCores() { return cpusCores;  }

    public int getPythonMemory() { return pythonMemory; }

    public int getJobManagerMemory() { return jobManagerMemory;  }

    public int getDriverVC() {  return driverVC;  }

    public void setTaskManagerMemory(int taskManagerMemory) {
      this.taskManagerMemory = taskManagerMemory;
    }

    public boolean isAdvanceConfig() {
      return isAdvanceConfig;
    }

    public void setAdvanceConfig(boolean advanceConfig) {
      isAdvanceConfig = advanceConfig;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(io.hops.cli.action.JobCreateAction.class);
  private final JsonObject payload;
  private final HopsworksAPIConfig hopsworksAPIConfig;
  private final String jobName;

  public JobCreateAction(HopsworksAPIConfig hopsworksAPIConfig, String jobName) throws IOException {
    this(hopsworksAPIConfig, jobName, new JobCreateAction.Args());
  }

  public JobCreateAction(HopsworksAPIConfig hopsworksAPIConfig, String jobName, JobCreateAction.Args args) throws IOException {

    super(hopsworksAPIConfig, jobName);
    this.hopsworksAPIConfig=hopsworksAPIConfig;
    this.jobName=jobName;
    payload = getJobConfig(args);

  }

  private JsonObject getJobConfig(JobCreateAction.Args args) throws IOException {

    //inspect job config
    JsonObject jobConfig = null;
    switch (args.getJobType()) {
      case SPARK:  //inspect spark job config first
        JsonObject respConfig = inspectJobConfig(args.getJobType().toLowerCase(), args.getAppPath());
        JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
        for (String key : respConfig.keySet()) {
          objectBuilder.add(key, respConfig.get(key));
        }
        //add user job config
        objectBuilder.add("appPath", args.getAppPath());
        if (!respConfig.containsKey("mainClass")) // add only if inspectJob does not return main class, e.g for pure spark
          objectBuilder.add("mainClass", args.getMainClass());

        objectBuilder.add("defaultArgs", Json.createValue(args.getCommandArgs()));
        objectBuilder.add("amMemory", Json.createValue(args.getDriverMemInMbs()));
        objectBuilder.add("amVCores", Json.createValue(args.getDriverVC()));
        objectBuilder.add("spark.executor.memory", Json.createValue(args.getExecutorMemInMbs()));
        objectBuilder.add("spark.executor.cores", Json.createValue(args.getExecutorVC()));
        objectBuilder.add("spark.dynamicAllocation.enabled", args.isDynamic());
        objectBuilder.add("spark.executor.instances", Json.createValue(args.getNumExecutors()));
        if (args.isDynamic()) {
          objectBuilder.add("spark.dynamicAllocation.minExecutors", args.getMinExecutors());
          objectBuilder.add("spark.dynamicAllocation.maxExecutors", args.getMaxExecutors());
          objectBuilder.add("spark.dynamicAllocation.initialExecutors", args.getInitExecutors());
        }
        if (args.isAdvanceConfig()) {
          objectBuilder.add("spark.yarn.dist.archives", args.getArchives());
          objectBuilder.add("spark.yarn.dist.pyFiles", args.getPythonDependency());
          objectBuilder.add("spark.yarn.dist.files", args.getFiles());
          objectBuilder.add("spark.yarn.dist.jars", args.getJars());
          objectBuilder.add("properties", args.getProperties());
        }

        jobConfig = objectBuilder.build();

        break;
      case PYTHON:
        jobConfig = getPythonJobConfig(args);
        break;
      case FLINK:
        jobConfig = getFlinkJobConfig(args);
        break;
    }

    return jobConfig;

  }


  public JsonObject getFlinkJobConfig(JobCreateAction.Args args){

    JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
    objectBuilder.add("type", FLINK_JOB_CONFIG)
        .add("amQueue", "default")
        .add("jobmanager.heap.size",args.getJobManagerMemory())
        .add("amVCores", 1)
        .add("numberOfTaskManagers",args.getNumTaskManager())
        .add("taskmanager.heap.size",args.getTaskManagerMemory())
        .add("taskmanager.numberOfTaskSlots",args.getNumSlots())
        .add("appName",jobName)
        .add("properties",args.getProperties());
    //.add("localResources","");

    return objectBuilder.build();

  }

  public JsonObject getPythonJobConfig(JobCreateAction.Args args){

    JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
    objectBuilder.add("type",PYTHON_JOB_CONFIG )
        .add("appName",jobName)
        .add("memory",args.getPythonMemory())
        .add("cores",args.getCpusCores())
        .add("jobType",PYTHON)
        .add("appPath",args.getAppPath())
        .add("defaultArgs",args.getCommandArgs())
        .add("files",args.getFiles());
    //.add("localResources","");
    return objectBuilder.build();

  }


  @Override
  public int execute() throws Exception {

    CloseableHttpClient getClient = getClient();

    HttpPut request = new HttpPut(getJobUrl());
    request.addHeader("User-Agent", USER_AGENT);
    request.addHeader("Authorization", "ApiKey " + hopsworksAPIConfig.getApiKey());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");

    StringEntity entity = new StringEntity(payload.toString());
    request.setEntity(entity);

    CloseableHttpResponse response = getClient.execute(request);
    int status = readJsonResponse(response);
    response.close();
    return status;
  }


}