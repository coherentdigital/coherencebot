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
package org.apache.nutch.indexwriter.s3;

import java.lang.invoke.MethodHandles;
import java.time.format.DateTimeFormatter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends NutchDocuments to a configured S3 Bucket.
 */
public class S3IndexWriter implements IndexWriter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration config;
  private String s3Bucket;
  private String s3Folder;
  private String s3RegionString;
  private String awsAccessKeyId;
  private String awsSecretAccessKey;
  private AmazonS3 s3 = null;

  @Override
  public void open(Configuration conf, String name) throws IOException {
    // Implementation not required
  }

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters
   *          Params from the index writer configuration.
   * @throws IOException
   *           Some exception thrown by writer.
   */
  @Override
  public void open(IndexWriterParams parameters) throws IOException {

    s3Bucket = parameters.get(S3Constants.BUCKET);
    if (StringUtils.isBlank(s3Bucket)) {
      String message = "Missing s3 bucket this should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    s3RegionString = parameters.get(S3Constants.REGION);
    if (StringUtils.isBlank(s3RegionString)) {
      String message = "Missing s3 region this should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    s3Folder = parameters.get(S3Constants.FOLDER);
    if (StringUtils.isBlank(s3Folder)) {
      String message = "Missing s3 folder this should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    awsAccessKeyId = parameters.get(S3Constants.AWS_ACCESS_KEY_ID);
    if (StringUtils.isBlank(awsAccessKeyId)) {
      String message = "Missing s3 aws_access_key_id this should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    awsSecretAccessKey = parameters.get(S3Constants.AWS_SECRET_ACCESS_KEY);
    if (StringUtils.isBlank(awsSecretAccessKey)) {
      String message = "Missing s3 aws_secret_access_key this should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    s3 = makeClient(parameters);
  }

  /**
   * Generates a S3Client
   * 
   * @param parameters
   *          implementation specific
   *          {@link org.apache.nutch.indexer.IndexWriterParams}
   * @return an initialized {@link org.elasticsearch.client.RestHighLevelClient}
   * @throws IOException
   *           if there is an error reading the
   *           {@link org.apache.nutch.indexer.IndexWriterParams}
   */
  protected AmazonS3 makeClient(IndexWriterParams parameters)
      throws IOException {

    BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKeyId,
        awsSecretAccessKey);
    s3 = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withRegion(Regions.fromName(s3RegionString)).build();

    return s3;
  }

  @Override
  public void write(NutchDocument doc) throws IOException {

    String collectionId = (String) doc.getFieldValue("collection_slug");
    if (StringUtils.isBlank(collectionId)) {
      collectionId = "unknown-collection";
    }

    JSONObject jDoc = new JSONObject();

    // Add each field of this doc to the index builder
    try {
      String id = (String) doc.getFieldValue("id");
      LOG.info("Writing document " + id);
      for (final Map.Entry<String, NutchField> e : doc) {
        LOG.info("Adding " + e.getKey());
        final List<Object> values = e.getValue().getValues();

        if (values.size() > 1) {
          jDoc.put(e.getKey(), values);
        } else {
          Object value = values.get(0);
          if (value instanceof java.util.Date) {
            value = DateTimeFormatter.ISO_INSTANT
                .format(((java.util.Date) value).toInstant());
          }
          jDoc.put(e.getKey(), value);
        }
      }
    } catch (JSONException e1) {
      LOG.error(e1.getMessage(), e1);
    }

    try {
      String jsonString = jDoc.toString(2);
      LOG.info("Writing json " + jsonString);
      ObjectMetadata s3Meta = new ObjectMetadata();
      s3Meta.setContentType("application/json");
      UUID uuid = UUID.randomUUID();
      String s3Key = s3Folder + "/" + collectionId + "-" + uuid.toString();
      LOG.info("key: " + s3Key);
      byte[] contentAsBytes = jsonString.getBytes("UTF-8");
      ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(
          contentAsBytes);
      s3Meta.setContentLength(contentAsBytes.length);
      s3.putObject(
          new PutObjectRequest(s3Bucket, s3Key, contentsAsStream, s3Meta));
    } catch (AmazonServiceException e) {
      LOG.error(e.getMessage(), e);
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
    }
  }

  @Override
  public void delete(String key) throws IOException {
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void commit() throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Returns {@link Map} with the specific parameters the IndexWriter instance
   * can take.
   *
   * @return The values of each row. It must have the form
   *         &#60;KEY,&#60;DESCRIPTION,VALUE&#62;&#62;.
   */
  @Override
  public Map<String, Map.Entry<String, Object>> describe() {

    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();
    properties.put(S3Constants.BUCKET, new AbstractMap.SimpleEntry<>(
        "S3 Bucket", this.s3Bucket == null ? "" : this.s3Bucket));
    properties.put(S3Constants.FOLDER, new AbstractMap.SimpleEntry<>(
        "The S3 folder to store results in", this.s3Folder == null ? "" : this.s3Folder));
    properties.put(S3Constants.REGION, new AbstractMap.SimpleEntry<>(
        "The AWS region of the bucket", this.s3RegionString == null ? "" : this.s3RegionString));
    properties.put(S3Constants.AWS_ACCESS_KEY_ID, new AbstractMap.SimpleEntry<>(
        "The AWS Access Key ID for accessing the bucket", this.awsAccessKeyId == null ? "" : this.awsAccessKeyId));
    properties.put(S3Constants.AWS_SECRET_ACCESS_KEY, new AbstractMap.SimpleEntry<>(
        "The AWS Secret Access Key credentials for accessing the bucket", this.awsSecretAccessKey == null ? "" : this.awsSecretAccessKey));

    return properties;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override
  public Configuration getConf() {
    return config;
  }
}
