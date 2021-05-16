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
import java.util.ArrayList;
import java.util.Iterator;
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
import org.codehaus.jettison.json.JSONArray;
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

  /**
   * Find the object in a JSONObject with the specified keyPath.
   *
   * The find is capable of finding objects that are nested objects
   * or list of objects.  Array searching is limited to finding
   * either the first object in the array with the matching key
   * or (if its a 'props' array), the member that has the request key.
   *
   * @param keyPath
   *          a dot-notation path to the object, if we are searching for a prop,
   *          it's path element will be key=name
   * @param jParent
   *          the JSONObject we are searching in
   * @return the leaf-node object containing the final path element.
   * @throws JSONException
   */
  private Object find(String keyPath, JSONObject jParent) throws JSONException {

    // Split the dot notation into individual keys that we will search for iteratively.
    String[] keys = keyPath.split("\\.");
    Object obj = jParent;

    // If we are searching for a prop (key/value), the path will be key=name
    boolean isProp = false;
    // The key name we are looking for in a key/value pair.
    String name = null;

    // For every path element in the dot notation, find the object corresponding to that path.
    for (int pathIndex = 0; pathIndex < keys.length; pathIndex++) {
      String key = keys[pathIndex];

      // True if we are at the last element in the dot notation.
      boolean isLastPath = (pathIndex + 1 >= keys.length) ? true : false;

      // If the path element has an equals in it, it is a property name.
      if (key.indexOf("=") > 0) {
        String[] parts = key.split("=");
        key = parts[0];
        name = parts[1];
        isProp = true;
      }

      // An object of null indicates a not-found situation.
      if (obj == null) {
        break;

      // This loop will encounter JSONObjects and JSONArrays.
      } else if (obj instanceof JSONObject) {
        JSONObject jo = (JSONObject) obj;
        if (jo.has(key)) {
          if (isLastPath) {
            break;
          } else {
            obj = jo.get(key);
          }
        } else {
          obj = null;
        }

      } else if (obj instanceof JSONArray) {
        JSONArray ja = (JSONArray) obj;
        // If the JSON array is empty, we don't need to search it.
        if (ja.length() == 0) {
          obj = ja;
        } else {
          boolean foundObj = false;
          for (int i = 0; i < ja.length(); i++) {
            JSONObject jo = ja.getJSONObject(i);
            if (isProp && jo.has("key") && jo.has("value") && name.equals(jo.getString("key"))) {
              if (isLastPath) {
                obj = jo;
              }
              foundObj = true;
              break;
            } else if (!isProp && jo.has(key)) {
              if (isLastPath) {
                obj = jo;
              } else {
                obj = jo.get(key);
              }
              foundObj = true;
              break;
            } else if ( i + 1 >= ja.length()) {
              obj = null;
            }
          }
          if (ja.length() > 0 && !foundObj) {
            obj = null;
          }
        }
      } else {
        LOG.warn("Found " + obj.getClass());
      }
    }
    return obj;
  }

  /**
   * A function to clean out JSONObject keys with empty strings or null value or empty array.
   * Runs recursively through nested JSONObjects within the provided Object.
   *
   * @param jDoc a JSONObject
   * @return the cleaned JSONObject or null if it needs to be removed in entirety.
   * @throws JSONException
   */
  private JSONObject clean(JSONObject jDoc) throws JSONException {
    if (jDoc == null) {
      return jDoc;
    }

    // This document will be the reconstructed object with empty values skipped.
    JSONObject jReturn = new JSONObject();

    @SuppressWarnings("unchecked")
    Iterator<String> keys = jDoc.keys();

    // Check each attribute in the object.
    while (keys.hasNext()) {
      String key = keys.next();
      Object obj = jDoc.get(key);

      if (obj == null) {
        // Remove any null object.
        continue;

      } else if (obj instanceof String) {
        // Remove any zero-length string.
        String str = (String) obj;
        if (str.trim().length() > 0) {
          jReturn.put(key, obj);
        }

      } else if (obj instanceof JSONObject) {
        // Make a new object, removing any empty attributes.
        JSONObject jo = (JSONObject) obj;
        JSONObject jNew = clean(jo);
        // Save the new object only if it has attributes.
        if (jNew != null && jNew.length() > 0) {
          jReturn.put(key, jNew);
        }

      } else if (obj instanceof JSONArray) {
        // Make a new array, removing any empty elements.
        // We inspect String arrays, JSONObject arrays and props arrays.
        // Integer arrays are left unmodified.
        JSONArray jaOld = (JSONArray) obj;
        JSONArray jaNew = new JSONArray();
        for (int i = 0; i < jaOld.length(); i++) {
          Object arrayElem = jaOld.get(i);
          Object jNew = null;
          if (arrayElem == null) {
            continue;
          } else if (arrayElem instanceof JSONObject) {
            JSONObject jo  = (JSONObject) arrayElem;
            // Is it a kv prop with an empty value?
            if (jo.has("key") && jo.has("value")) {
              if ("".equals(jo.getString("value")) || null == jo.getString("value")) {
                // Skipping prop which has an empty value
                continue;
              } else {
                jNew = jo;
              }
            } else {
              // Other object types get a recursive clean.
              jo = clean(jo);
              // If the cleaned object has attributes, retain it.
              if (jo.length() > 0) {
                jNew = jo;
              }
            }
          } else if (arrayElem instanceof String) {
            String str = (String) arrayElem;
            if (str != null && str.trim().length() > 0) {
              jNew = arrayElem;
            }
          } else {
            // Other object types in an array are retained without inspection.
            jNew = arrayElem;
          }
          // If the new object is not null, add it back in.
          if (jNew != null) {
            jaNew.put(jNew);
          }
        }
        // If the new array is non-zero length, retain it
        if (jaNew.length() > 0) {
          jReturn.put(key, jaNew);
        }
      }
    }

    // Return the cleaned object.
    return jReturn;
  }

  @Override
  public void write(NutchDocument doc) throws IOException {

    String template = "{\"collection\":{\"title\":\"\",\"slug\":\"\",\"organization\":\"\"},\"harvest\":{\"id\":\"uuid\",\"date\":\"\"},\"artifacts\":[{\"id\":\"uuid\",\"uri\":\"\",\"title\":\"\",\"identifier\":\"\",\"date_updated\":\"\",\"date_published\":\"\",\"publisher\":\"\",\"publication_place\":\"\",\"summary\":\"\",\"type\":\"\",\"authors\":[{\"name\":\"\"}],\"languages\":[],\"thumbnail\":{\"url\":\"\"},\"full_text\":\"\",\"files\":[{\"url\":\"\",\"size\":0,\"media_type\":\"\"}],\"hidden_props\":[{\"key\":\"title_algorithm\",\"value\":\"\"},{\"key\":\"anchor\",\"value\":\"\"},{\"key\":\"heading\",\"value\":\"\"},{\"key\":\"segment\",\"value\":\"\"},{\"key\":\"host\",\"value\":\"\"},{\"key\":\"domain\",\"value\":\"\"},{\"key\":\"anchorLength\",\"value\":0},{\"key\":\"headingLength\",\"value\":0},{\"key\":\"titleLength\",\"value\":0},{\"key\":\"depth\",\"value\":0}],\"props\":[{\"key\":\"Pages\",\"value\":0}]}]}";
    UUID uuid = UUID.randomUUID();
    template = template.replace("uuid", uuid.toString());
    // Collection UUID _ artifact UUID.json (for meta)
    // Collection UUID _ artifact UUID.txt (for full text)

    String collectionId = (String) doc.getFieldValue("collection.slug");
    if (StringUtils.isBlank(collectionId)) {
      collectionId = "unknown-collection";
    }

    // Add each field of this Nutch doc to a JSON representation according to the
    // template.
    // NutchDocuments are flat key=value, or key=array, but the Commons schema is nested.
    // So this section of code builds a nested object from a template with empty values.
    // It then hydrates it by finding the object with the desired path in the template
    // and then pasting in the value from the flat NutchDocument.
    //
    // Field names in the NutchDocument have been mapped to a Commons schema
    // field.
    //
    // The field names in the NutchDocument use a dot notation to indicate their output path. 
    // See conf/indexwriters.xml for the mapping.
    try {
      JSONObject jDoc = new JSONObject(template);

      for (final Map.Entry<String, NutchField> e : doc) {
        String key = e.getKey();

        // Find this element in our template doc.
        Object elem = find(key, jDoc);

        if (elem != null) {
          // The last path on the dot notation key is the key for the nested object.
          String[] objKeys = key.split("\\.");
          String objKey = objKeys[objKeys.length - 1];
          if (objKey.indexOf("=") > 0) {
            // props save the result in 'value'
            objKey = "value";
          }
          final List<Object> values = e.getValue().getValues();
          // It the NutchDocument field single-valued or multi-valued?
          if (values.size() > 1) {
            // If the matched output is an array, append to it.
            if (elem instanceof JSONArray) {
              JSONArray ja = (JSONArray) elem;
              for (Object value : values) {
                ja.put(value);
              }

            // If the match output is an object, see if the target attribute is an array.
            } else if (elem instanceof JSONObject) {
              JSONObject jo = (JSONObject) elem;
              boolean isPropertyAnArray = (jo.get(objKey) instanceof JSONArray) ? true : false;
              if (isPropertyAnArray) {
                ArrayList<Object> al = new ArrayList<Object>();
                for (Object value : values) {
                  al.add(value);
                }
                jo.put(objKey, al);
              } else {
                LOG.warn("Attempt to put multi-valued Nutch field into a scalar Commons attribute: " + key + " into " + objKey + ", saving first elem only.");
                jo.put(objKey, values.get(0));
              }
            }
          } else {
            Object value = values.get(0);
            if (value instanceof java.util.Date) {
              value = DateTimeFormatter.ISO_INSTANT
                  .format(((java.util.Date) value).toInstant());
            }
            if (elem instanceof JSONArray) {
              JSONArray ja = (JSONArray) elem;
              ja.put(value);
            } else if (elem instanceof JSONObject) {
              JSONObject jo = (JSONObject) elem;
              boolean isPropertyAnArray = (jo.get(objKey) instanceof JSONArray) ? true : false;
              boolean isPropertyAnInteger = (jo.get(objKey) instanceof Integer) ? true : false;
              if (isPropertyAnArray) {
                ArrayList<Object> al = new ArrayList<Object>();
                al.add(value);
                jo.put(objKey, al);
              } else if (isPropertyAnInteger) {
                if (value instanceof Integer) {
                  jo.put(objKey, value);
                } else if (value instanceof String) {
                  try {
                    Integer intValue = Integer.valueOf((String)value);
                    jo.put(objKey, intValue);
                  } catch (NumberFormatException nfe) {
                    LOG.error("Attempt to place a non numeric value into an integer attribute: " + objKey + " from " + value);
                  }
                }
              } else {
                jo.put(objKey, value);
              }
            } else {
              LOG.warn("Find returned a " + elem.getClass());
            }
          }
        } else {
          LOG.error("Could not find key " + key);
        }
      }


      // Go through the template doc and remove any empty fields
      jDoc = clean(jDoc);

      String jsonString = jDoc.toString(2);
      // LOG.info("Writing json " + jsonString);
      ObjectMetadata s3Meta = new ObjectMetadata();
      s3Meta.setContentType("application/json");
      String s3Key = s3Folder + "/" + collectionId + "-" + uuid.toString()
          + ".json";
      LOG.info("key: " + s3Key);
      byte[] contentAsBytes = jsonString.getBytes("UTF-8");
      ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(
          contentAsBytes);
      s3Meta.setContentLength(contentAsBytes.length);
      s3.putObject(
          new PutObjectRequest(s3Bucket, s3Key, contentsAsStream, s3Meta));
    } catch (JSONException e1) {
      LOG.error(e1.getMessage(), e1);
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
    properties.put(S3Constants.FOLDER,
        new AbstractMap.SimpleEntry<>("The S3 folder to store results in",
            this.s3Folder == null ? "" : this.s3Folder));
    properties.put(S3Constants.REGION,
        new AbstractMap.SimpleEntry<>("The AWS region of the bucket",
            this.s3RegionString == null ? "" : this.s3RegionString));
    properties.put(S3Constants.AWS_ACCESS_KEY_ID,
        new AbstractMap.SimpleEntry<>(
            "The AWS Access Key ID for accessing the bucket",
            this.awsAccessKeyId == null ? "" : this.awsAccessKeyId));
    properties.put(S3Constants.AWS_SECRET_ACCESS_KEY,
        new AbstractMap.SimpleEntry<>(
            "The AWS Secret Access Key credentials for accessing the bucket",
            this.awsSecretAccessKey == null ? "" : this.awsSecretAccessKey));

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
