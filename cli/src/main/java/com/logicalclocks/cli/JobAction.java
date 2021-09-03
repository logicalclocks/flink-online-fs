package com.logicalclocks.cli;

import io.hops.cli.config.HopsworksAPIConfig;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.CloseableHttpClient;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import static org.apache.http.HttpHeaders.USER_AGENT;

public abstract class JobAction extends HopsworksAction {
  protected final String jobName;
  protected String result;
  protected JsonObject jsonResult;

  public JobAction(HopsworksAPIConfig hopsworksAPIConfig, String jobName) {
    super(hopsworksAPIConfig);
    this.jobName = jobName;
  }

  protected void addHeaders(HttpRequestBase request) {
    request.addHeader("User-Agent", USER_AGENT);
    request.addHeader("Authorization", "ApiKey " + hopsworksAPIConfig.getApiKey());
  }

  protected HttpPost getJobPost(String path) throws IOException {
    HttpPost request = new HttpPost(getJobUrl() + "/" + path);
    addHeaders(request);
    return request;
  }

  protected HttpPut getJobPut(String path) throws IOException {
    HttpPut request = new HttpPut(getJobUrl() + "/" + path);
    addHeaders(request);
    return request;
  }

  protected HttpGet getJobGet(String path) throws IOException {
    HttpGet request = new HttpGet(getJobUrl() + "/" + path);
    addHeaders(request);
    return request;
  }
  public String getJobUrl()  throws IOException {
    return hopsworksAPIConfig.getProjectUrl() + this.getProjectId() + "/jobs/" + jobName;
  }

  public String getJobName() {
    return jobName;
  }

  public String getResult() {
    return result;
  }

  protected void setResult(String result) {
    this.result = result;
  }

  public JsonObject getJsonResult() {
    return jsonResult;
  }

  protected void setJsonResult(JsonObject jsonResult) {
    this.jsonResult = jsonResult;
  }

  protected int readJsonResponse(CloseableHttpResponse response) throws IOException {
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
    StringBuilder res = new StringBuilder();
    String line;
    while ((line = rd.readLine()) != null) {
      res.append(line);
    }
    JsonReader jsonReader = Json.createReader(new StringReader(res.toString()));
    JsonObject body = jsonReader.readObject();
    jsonReader.close();
    response.close();
    setResult(res.toString());
    setJsonResult(body);
    return response.getStatusLine().getStatusCode();
  }

  /**
   *
   * @return id of latest execution of the current job
   * @throws IOException if problem getting the job's executions from Hopsworks
   */
  protected int getLatestExecution() throws IOException {
    // First get the most recent execution for this job. Then get the log/path for that execution.
    CloseableHttpClient client = getClient();
    //HttpGet request = getJobGet("/executions?sort_by=appId:desc&limit=1");
    HttpGet request = getJobGet("executions?sort_by=appId:desc&limit=1");

    CloseableHttpResponse response = client.execute(request);
    int status = readJsonResponse(response);
    if (status != HttpStatus.SC_OK && status != HttpStatus.SC_CREATED) {
      client.close();
      throw new IOException("Could not get latest execution: " + status);
    }

    JsonArray resultArray = this.getJsonResult().getJsonArray("items");
    if (resultArray==null){
      return 0;
    }
    JsonObject execution = resultArray.getJsonObject(0);
    client.close();
    response.close();
    return execution.getInt("id");
  }

  public String getJobsOnlyUrl()  throws IOException {
    return hopsworksAPIConfig.getProjectUrl() + this.getProjectId() + "/jobs/";
  }

  /**
   * get user job configuration for spark
   * @param jobType
   * @param path
   * @return
   * @throws IOException
   */
  protected JsonObject inspectJobConfig(String jobType,String path) throws IOException {
    path=path.split("hdfs://")[1];
    CloseableHttpClient client = getClient();
    HttpGet request = new HttpGet(getJobsOnlyUrl()+jobType+"/inspection?path="+path);
    addHeaders(request);
    CloseableHttpResponse response = client.execute(request);
    int status = readJsonResponse(response);
    if (status != HttpStatus.SC_OK && status != HttpStatus.SC_CREATED) {
      client.close();
      throw new IOException("Could not inspect job configuration: " + status);
    }

    return getJsonResult();

  }

  protected int getExecutionById(Integer execId) throws IOException {
    CloseableHttpClient client = getClient();
    HttpGet request = getJobGet("/executions/"+execId.toString());

    CloseableHttpResponse response = client.execute(request);
    int status = readJsonResponse(response);
    if (status != HttpStatus.SC_OK && status != HttpStatus.SC_CREATED) {
      client.close();
      throw new IOException("Could not get execution id: " + execId +" ; Status :"+status);
    }

    client.close();
    response.close();
    return status;
  }

  /**
   * check if job name exists
   * @return true if found
   * @throws IOException
   */
  public boolean getJobExists() throws IOException {
    CloseableHttpClient getClient = getClient();
    HttpGet request = getJobGet("");
    CloseableHttpResponse response = getClient.execute(request);
    int statusCode = readJsonResponse(response);
    return statusCode != HttpStatus.SC_NOT_FOUND;
  }
}
