/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.statswriter.elastic;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.util.NutchConfiguration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Index runtime statistics to a dashboard. */
public class StatsIndexer extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private String host = "";
  private int port = 80;
  private boolean auth = false;
  private String index = "";
  private String username = "";
  private String password = "";
  private String input = "";
  private RestHighLevelClient client;
  private BulkProcessor bulkProcessor;

  private int maxBulkDocs = 250;
  private int maxBulkLength = 2500500;
  private int expBackoffMillis = 100;
  private int expBackoffRetries = 10;
  private long bulkCloseTimeout = 600;

  public static class InputCompatMapper extends
      Mapper<WritableComparable<?>, Writable, Text, NutchWritable> {
    
    private Text newKey = new Text();

    @Override
    public void map(WritableComparable<?> key, Writable value,
        Context context) throws IOException, InterruptedException {
      // convert on the fly from old formats with UTF8 keys.
      // UTF8 deprecated and replaced by Text.
      if (key instanceof Text) {
        newKey.set(key.toString());
        key = newKey;
      }
      context.write((Text) key, new NutchWritable(value));
    }

  }

  public void indexStats() throws IOException,
      InterruptedException, ClassNotFoundException {

    LOG.info("StatsIndexer: indexing {} to {}/{}", input, host, index);
    int records_indexed = 0;
    try {
      client = makeClient();
      LOG.debug("Creating BulkProcessor with maxBulkDocs={}, maxBulkLength={}",
          maxBulkDocs, maxBulkLength);
      bulkProcessor = BulkProcessor
          .builder((request, bulkListener) -> client.bulkAsync(request,
              RequestOptions.DEFAULT, bulkListener), bulkProcessorListener())
          .setBulkActions(maxBulkDocs)
          .setBulkSize(new ByteSizeValue(maxBulkLength, ByteSizeUnit.BYTES))
          .setConcurrentRequests(1)
          .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
              TimeValue.timeValueMillis(expBackoffMillis), expBackoffRetries))
          .build();

      LOG.debug("Creating BulkProcessor with maxBulkDocs={}, maxBulkLength={}",
          maxBulkDocs, maxBulkLength);
      Path fileName = FileSystems.getDefault().getPath(".", input);
      String jsonStr = new String(Files.readAllBytes(fileName));
      JSONArray ja = new JSONArray(jsonStr);
      if (ja.length() > 0) {
        for (int i = 0; i < ja.length(); i++) {
          try {
            JSONObject jo = ja.getJSONObject(i);
            write(jo);
            records_indexed++;
          } catch (Exception e) {
           LOG.error(e.getMessage());
          }
        }
      }
      commit();
      close();
      LOG.info("Indexed {} records.", records_indexed);
    } catch (Exception e) {
      LOG.error(e.toString());
    }

    LOG.info("StatsIndexer: done");
  }

  /**
   * Generates a default BulkProcessor.Listener
   * @return {@link BulkProcessor.Listener}
   */
  protected BulkProcessor.Listener bulkProcessorListener() {
    return new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
          Throwable failure) {
        LOG.error("Elasticsearch indexing failed:", failure);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
          BulkResponse response) {
        if (response.hasFailures()) {
          LOG.warn("Failures occurred during bulk request: {}",
              response.buildFailureMessage());
        }
      }
    };
  }


  protected void write(JSONObject jo) throws IOException, JSONException {
    String id = null;
    if (jo.has("id")) {
      id = jo.getString("id");
    } else if (jo.has("url")) {
      id = jo.getString("url");
    }
    if (id == null) {
      throw new JSONException("JSON object does not contain an 'id' or 'url");
    }

    // Add each field of this doc to the index builder
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

    @SuppressWarnings("unchecked")
    Iterator<String> keys = jo.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      Object value = jo.get(key);
      if (value instanceof JSONArray) {
        JSONArray arrayObj = (JSONArray)value;
        builder.startArray(key);
        for (int i = 0; i < arrayObj.length(); i++) {
          builder.value(arrayObj.get(i));
        }
        builder.endArray();
      } else {
        builder.field(key, value);
      }
    }
    builder.endObject();

    IndexRequest request = new IndexRequest(index).id(id).source(builder);
    request.opType(DocWriteRequest.OpType.INDEX);

    bulkProcessor.add(request);
  }

  protected void commit() throws IOException {
    bulkProcessor.flush();
  }

  protected void close() throws IOException {
    // Close BulkProcessor (automatically flushes)
    try {
      bulkProcessor.awaitClose(bulkCloseTimeout, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("interrupted while waiting for BulkProcessor to complete ({})",
          e.getMessage());
    }

    client.close();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage();
      return -1;
    }

    // collect general options
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-host")) {
        args[i] = null;
        if (i < args.length) {
          i++;
          host = args[i];
          args[i] = null;
        }
      } else if (args[i].equals("-index")) {
        args[i] = null;
        args[i] = null;
        if (i < args.length) {
          i++;
          index = args[i];
          args[i] = null;
        }
      } else if (args[i].equals("-username")) {
        args[i] = null;
        args[i] = null;
        if (i < args.length) {
          i++;
          username = args[i];
          args[i] = null;
        }
      } else if (args[i].equals("-password")) {
        args[i] = null;
        args[i] = null;
        if (i < args.length) {
          i++;
          password = args[i];
          args[i] = null;
        }
      } else if (args[i].equals("-input")) {
        args[i] = null;
        args[i] = null;
        if (i < args.length) {
          i++;
          input = args[i];
          args[i] = null;
        }
      }
    }
    indexStats();
    return 0;
  }

  /**
   * Generates a RestHighLevelClient for the host
   * @return an initialized {@link org.opensearch.client.RestHighLevelClient}
   * @throws IOException if there is an error reading the 
   * {@link org.apache.nutch.indexer.IndexWriterParams}
   */
  protected RestHighLevelClient makeClient()
      throws IOException {

    if (username != null && username.length() > 0 && 
        password != null && password.length() > 0) {
      auth = true;
    }

    client = null;

    if (host != null && port > 1 && auth) {
      final HttpHost esHost = new HttpHost(host, 443, "https");
      final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
        new AuthScope(esHost),
        new UsernamePasswordCredentials(username, password)
      );

      //Initialize the client with SSL and TLS enabled
      final RestClientBuilder restClientBuilder = RestClient.builder(esHost)
          .setHttpClientConfigCallback(
              new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(
                    HttpAsyncClientBuilder httpClientBuilder) {
                  return httpClientBuilder
                      .setDefaultCredentialsProvider(credentialsProvider);
                }
              });
      client = new RestHighLevelClient(restClientBuilder);
    } else {
      throw new IOException(
        "OpenSearch RestHighLevelClient initialization Failed!!!\\n\\nPlease Provide the host, username and password"
      );
    }
    return client;
  }

  private static void usage() {
    System.err
        .println("Usage: StatsIndexer [general options]\n");
    System.err.println("* General options:");
    System.err.println("\t-host\tElastic host");
    System.err.println("\t-index\tElastic index");
    System.err.println("\t-username\tAuth username");
    System.err.println("\t-password\tAuth password");
    System.err.println("\t-file\tInput file");
    System.err.println();
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new StatsIndexer(), args);
    System.exit(result);
  }
}
