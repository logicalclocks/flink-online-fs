package com.logicalclocks.cli;

import io.hops.cli.config.HopsworksAPIConfig;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

public class SubmitFlinkJob extends JobAction {


  private static final Logger logger = LoggerFactory.getLogger(JobRunAction.class);
  private final String url;
  protected int userExecId = 0;
  protected String local_file_path = "";
  protected String mainClass = "";
  protected String userArgs = "";
  HopsworksAPIConfig hopsworksAPIConfig;
  private URI filePath;

  public SubmitFlinkJob(HopsworksAPIConfig hopsworksAPIConfig, String jobName) {
    super(hopsworksAPIConfig, jobName);
    this.hopsworksAPIConfig = hopsworksAPIConfig;
    url = hopsworksAPIConfig.getApiUrl();
  }

  public void setUserExecId(int userExecId) {
    this.userExecId = userExecId;
  }

  public String getLocal_file_path() {
    return local_file_path;
  }

  public void setLocal_file_path(String local_file_path) {
    this.local_file_path = local_file_path;
  }

  public String getMainClass() {
    return mainClass;
  }

  public void setMainClass(String mainClass) {
    this.mainClass = mainClass;
  }

  private String getUserArgs() {
    return userArgs;
  }

  public void setUserArgs(String userArgs) throws UnsupportedEncodingException {
    this.userArgs = URLEncoder.encode(userArgs, "utf8");
  }

  private void initPath(String filePath) throws URISyntaxException {
    URI path;
    // Default FS if not given, is the local FS (file://)
    if (SystemUtils.IS_OS_WINDOWS && !filePath.startsWith("file://")) {
      path = new URI("file:///" + filePath);
    } else if (filePath.startsWith("/")) {
      path = new URI("file://" + filePath);
    } else {
      path = new URI(filePath);
    }
    this.filePath = path;

  }

  /**
   * Executes the complete request to submit flink program
   * <p>
   * 1.check for existing RUNNING flink job.
   * 2.If not found first submit a flink job run
   * and wait till its in RUNNING mode
   * 3.Use the appId of this running job
   * 4.Upload jar to flink master
   * 5.Submit program
   *
   * @return final status code of Http response
   * @throws Exception
   */
  @Override
  public int execute() throws Exception {

    String app_id = "", jarId, previousExecStatus = "";
    JsonArray resultArray;
    initPath(getLocal_file_path());
    //check last execution
    if (userExecId == 0) {
      getLatestExecution();
    } else {
      getExecutionById(userExecId);
    }
    resultArray = this.getJsonResult().getJsonArray("items");
    if (resultArray != null) {
      previousExecStatus = resultArray.get(0).asJsonObject().getString("state");

    }
    //check status of previous run
    if (previousExecStatus != null
        && !(previousExecStatus.equals("RUNNING") || previousExecStatus.equals("INITIALIZING") ||
        previousExecStatus.equals("ACCEPTED") || previousExecStatus.equals("NEW") ||
        previousExecStatus.equals("NEW_SAVING") || previousExecStatus.equals("SUBMITTED"))) {
      // no previous running job, start a new job run
      JobRunAction runJob = new JobRunAction(hopsworksAPIConfig, jobName, userArgs);
      int status_cd_run = runJob.execute();
      if (status_cd_run != HttpStatus.SC_CREATED && status_cd_run != HttpStatus.SC_OK) {
        throw new Exception("Could not submit HTTP request to start job run");
      }
      // wait for 90secs till status is RUNNING
      int wait = 90, wait_count = 0,sleep_time=5;
      String runStatus = "";
      while (wait_count < wait && !runStatus.equals("RUNNING")) {
        TimeUnit.SECONDS.sleep(sleep_time);
        wait_count += sleep_time;
        getLatestExecution(); // get last execution of job
        resultArray = this.getJsonResult().getJsonArray("items");
        runStatus = resultArray.get(0).asJsonObject().getString("state");

      }

      if (!runStatus.equals("RUNNING")) {
        throw new Exception("Flink cluster did not start, check job logs for details");
      } else logger.info("Started Flink cluster with job name ", jobName);

    } else logger.info("Found Flink cluster with this name already running, will use it to submit the Flink job");

    resultArray = this.getJsonResult().getJsonArray("items");
    if (resultArray != null) {
      app_id = resultArray.get(0).asJsonObject().get("appId").toString();
      app_id = JSON.parse(app_id).toString();
    }
    String base_url = url + "/hopsworks-api/flinkmaster/" + app_id;
    //upload file
    CloseableHttpResponse responseUpload = executeUpload(base_url, this.filePath.getPath());
    int statusCode = responseUpload.getStatusLine().getStatusCode();
    if (statusCode != HttpStatus.SC_OK) {
      throw new Exception("HTTP File Upload not successful");
    }
    //read upload response
    readJsonResponse(responseUpload);
    String respFile = this.getJsonResult().getString("filename");
    String[] items = respFile.split("/");
    jarId = items[items.length - 1];
    base_url += "/jars/" + jarId + "/run?entry-class=" + getMainClass() + "&program-args=" + getUserArgs();
    logger.info("Submitting flink job to: " + base_url);
    // submit program
    statusCode = submitPostRequest(base_url);

    return statusCode;
  }

  /**
   * submit post request to given url
   *
   * @param url URL to hit
   * @return status code of HttpResponse
   * @throws IOException
   */
  public int submitPostRequest(String url) throws IOException {
    CloseableHttpClient httpClient = getClient();
    HttpPost request = new HttpPost(url);
    addHeaders(request);
    CloseableHttpResponse response = httpClient.execute(request);
    int status = readJsonResponse(response);
    if (status != HttpStatus.SC_CREATED && status != HttpStatus.SC_OK) {
      throw new IOException(response.getStatusLine().getReasonPhrase());
    }
    httpClient.close();
    response.close();


    return status;
  }

  /**
   * Multi-part Upload jar to flink master
   *
   * @param baseUrl  URL to hit
   * @param filePath local file path to upload
   * @return HttpResponse
   * @throws Exception
   */
  public CloseableHttpResponse executeUpload(String baseUrl, String filePath) throws Exception {
    CloseableHttpClient httpClient = getClient();
    String apiUrl = baseUrl + "/jars/upload";
    HttpPost uploadFile = new HttpPost(apiUrl);
    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    // attaches the file to the POST:
    File targetFile = new File(filePath);
    builder.addBinaryBody(
        "jarfile",
        new FileInputStream(targetFile),
        ContentType.APPLICATION_OCTET_STREAM,
        targetFile.getName()
    );

    HttpEntity multipart = builder.build();
    addHeaders(uploadFile);
    uploadFile.setEntity(multipart);

    return httpClient.execute(uploadFile);
  }
}
