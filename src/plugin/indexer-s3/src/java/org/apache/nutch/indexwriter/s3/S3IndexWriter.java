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
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.format.DateTimeFormatter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
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
  private String template;
  private String validator;
  private String s3InvalidFolder;
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
      String message = "Missing s3 bucket.  This should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    s3RegionString = parameters.get(S3Constants.REGION);
    if (StringUtils.isBlank(s3RegionString)) {
      String message = "Missing s3 region. This should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    s3Folder = parameters.get(S3Constants.FOLDER);
    if (StringUtils.isBlank(s3Folder)) {
      String message = "Missing s3 folder. This should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    template = parameters.get(S3Constants.TEMPLATE);
    if (StringUtils.isBlank(template)) {
      String message = "Missing JSON template. This should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    validator = parameters.get(S3Constants.VALIDATOR);
    s3InvalidFolder = parameters.get(S3Constants.INVALID_FOLDER);

    awsAccessKeyId = parameters.get(S3Constants.AWS_ACCESS_KEY_ID);
    if (StringUtils.isBlank(awsAccessKeyId)) {
      String message = "Missing s3 aws_access_key_id. This should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    awsSecretAccessKey = parameters.get(S3Constants.AWS_SECRET_ACCESS_KEY);
    if (StringUtils.isBlank(awsSecretAccessKey)) {
      String message = "Missing s3 aws_secret_access_key. This should be set in index-writers.xml ";
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
   * The find is capable of finding objects that are nested objects or list of
   * objects. Array searching is limited to finding either the first object in
   * the array with the matching key or (if its a 'props' array), the member
   * that has the request key.
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

    // Split the dot notation into individual keys that we will search for
    // iteratively.
    String[] keys = keyPath.split("\\.");
    Object obj = jParent;

    // If we are searching for a prop (key/value), the path will be key=name
    boolean isProp = false;
    // The key name we are looking for in a key/value pair.
    String name = null;

    // For every path element in the dot notation, find the object corresponding
    // to that path.
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
            if (isProp && jo.has("key") && jo.has("value")
                && name.equals(jo.getString("key"))) {
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
            } else if (i + 1 >= ja.length()) {
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
   * A function to clean out JSONObject keys with empty strings or null value or
   * empty array. Also cleans any attribute who's key name is 'url' by running a
   * URL cleaner. Runs recursively through nested JSONObjects within the
   * provided Object.
   *
   * @param jDoc
   *          a JSONObject
   * @return the cleaned JSONObject or null if it needs to be removed in
   *         entirety.
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
          if ("url".equalsIgnoreCase(key) || "uri".equalsIgnoreCase(key)) {
            String urlStr = escapeIllegalUrlCharacters(str);
            jReturn.put(key, urlStr);
          } else {
            jReturn.put(key, obj);
          }
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
            JSONObject jo = (JSONObject) arrayElem;
            // Is it a kv prop with an empty value?
            if (jo.has("key") && jo.has("value")) {
              if ("".equals(jo.getString("value"))
                  || null == jo.getString("value")) {
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

  /**
   * Validate a JSON object using the validation endpoint.
   *
   * @param json
   * @return A message containing "OK" if valid, otherwise a message indicating
   *         the invalidating condition.
   */
  private String validate(String json) {
    String validatorResponse = "DEFAULTOK";
    InputStream in = null;
    OutputStream os = null;
    HttpURLConnection con = null;
    try {
      URL url = new URL(validator);
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setRequestProperty("Content-Type", "application/json; utf-8");
      con.setRequestProperty("Accept", "*/*");
      con.setConnectTimeout(10000);
      con.setReadTimeout(60000);
      con.setDoInput(true);
      con.setDoOutput(true);

      // POST the JSON to the validator
      byte[] jsonBytes = json.getBytes("UTF-8");
      con.setRequestProperty("Content-Length",
          Integer.toString(jsonBytes.length));
      os = con.getOutputStream();
      os.write(jsonBytes);
      os.flush();

      int responseCode = con.getResponseCode();
      if (responseCode < 400) {
        // Read the validation response into a string
        in = con.getInputStream();
      } else if (responseCode >= 400) {
        // Read the error response into a string
        in = con.getErrorStream();
        LOG.error("Returning " + responseCode + " from validator");
      }
      validatorResponse = IOUtils.toString(in, "UTF-8");
    } catch (Exception e) {
      LOG.error("Unable to validate using " + validator, e);
    } finally {
      try {
        if (os != null) {
          os.close();
        }
      } catch (IOException e) {
        LOG.error("Exception closing validator outputstream", e);
      }
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG.error("Exception closing validator inputstream", e);
      }
      try {
        if (con != null) {
          con.disconnect();
        }
      } catch (Exception e) {
        LOG.error("Exception closing validator connection", e);
      }
    }
    // If the validation response default has not been replaced by a correct
    // response or by an error, then we were unable to get a response.
    // In this case we default to "OK".
    // The best option at this point is to emit a warning and write the output
    // anyway. The S3 consumer can decide whether the file is OK or not.
    if (validatorResponse.equals("DEFAULTOK")) {
      LOG.error(
          "Unable to contact the S3 File Validator. Accepting the input by default");
    }
    return validatorResponse;
  }

  @Override
  public void write(NutchDocument doc) throws IOException {

    UUID uuid = UUID.randomUUID();
    template = template.replace("randomuuid", uuid.toString());
    // Collection UUID _ artifact UUID.json (for meta)
    // Collection UUID _ artifact UUID.txt (for full text)

    String collectionId = (String) doc.getFieldValue("collection.id");
    if (StringUtils.isBlank(collectionId)) {
      collectionId = "unknown-collection";
    }

    // If there's a field with the name 'uuid_seed'; we use it to generate a
    // UUID
    // In this way we can convert the NutchDoc's ID to a UUID
    String uuidSeed = (String) doc.getFieldValue("uuid_seed");
    if (uuidSeed != null && uuidSeed.length() > 0) {
      uuid = UUID.nameUUIDFromBytes(uuidSeed.getBytes());
      template = template.replace("uuid", uuid.toString());
      doc.removeField("uuid_seed");
    }

    // If the NutchDoc contains a field 'full_text_file', it is written
    // to its own text/plain .txt with the same UUID as the metadata
    String fullText = null;
    Object fullTextObj = doc.getFieldValue("full_text_file");
    if (fullTextObj != null) {
      if (fullTextObj instanceof String) {
        fullText = (String) fullTextObj;
        fullText = fullText.trim();
      } else {
        LOG.warn(
            "full_text_file is not a String.  Will not be saved to text/plain");
      }
      // The field is removed so it does not get added to the JSON.
      doc.removeField("full_text_file");
      // Writing of the file is deferred until below; after we have validated
      // the JSON.
    }

    // Add each field of this Nutch doc to a JSON representation according to
    // the template.
    // NutchDocuments are flat key=value, or key=array, but the template schema
    // may be nested.
    // So this section of code builds a nested object from a template with empty
    // values.
    // It then hydrates it by finding the object with the desired path in the
    // template and then pasting in the value from the flat NutchDocument.
    //
    // Field names in the NutchDocument have been mapped to the template schema
    // field.
    //
    // The field names in the NutchDocument use a dot notation to indicate their
    // output path.
    // See conf/indexwriters.xml for the mapping.
    try {
      JSONObject jDoc = new JSONObject(template);

      for (final Map.Entry<String, NutchField> e : doc) {
        String key = e.getKey();

        // Find this element in our template doc.
        Object elem = find(key, jDoc);

        if (elem != null) {
          // The last path on the dot notation key is the key for the nested
          // object.
          String[] objKeys = key.split("\\.");
          String objKey = objKeys[objKeys.length - 1];
          if (objKey.indexOf("=") > 0) {
            // props save the result in 'value'
            objKey = "value";
          }
          final List<Object> values = e.getValue().getValues();
          // Is the NutchDocument field single-valued or multi-valued?
          if (values.size() > 1) {
            // If the matched output is an array, append to it.
            if (elem instanceof JSONArray) {
              JSONArray ja = (JSONArray) elem;
              for (Object value : values) {
                ja.put(value);
              }

              // If the match output is an object, see if the target attribute
              // is an array.
            } else if (elem instanceof JSONObject) {
              JSONObject jo = (JSONObject) elem;
              boolean isPropertyAnArray = (jo.get(objKey) instanceof JSONArray)
                  ? true
                  : false;
              if (isPropertyAnArray) {
                ArrayList<Object> al = new ArrayList<Object>();
                for (Object value : values) {
                  al.add(value);
                }
                jo.put(objKey, al);
              } else {
                LOG.warn(
                    "Attempt to put a multi-valued Nutch field into a scalar attribute: "
                        + key + " into " + objKey
                        + ", concatenating elems.");
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < values.size(); i++) {
                  if (sb.length() > 0) {
                    sb.append("; ");
                  }
                  sb.append(values.get(i));
                }
                jo.put(objKey, sb.toString());
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
              boolean isPropertyAnArray = (jo.get(objKey) instanceof JSONArray)
                  ? true
                  : false;
              boolean isPropertyAnInteger = (jo.get(objKey) instanceof Integer)
                  ? true
                  : false;
              if (isPropertyAnArray) {
                ArrayList<Object> al = new ArrayList<Object>();
                al.add(value);
                jo.put(objKey, al);
              } else if (isPropertyAnInteger) {
                if (value instanceof Integer) {
                  jo.put(objKey, value);
                } else if (value instanceof String) {
                  try {
                    Integer intValue = Integer.valueOf((String) value);
                    jo.put(objKey, intValue);
                  } catch (NumberFormatException nfe) {
                    LOG.error(
                        "Attempt to place a non numeric value into an integer attribute: "
                            + objKey + " from " + value);
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

      // Go through the hydrated doc and remove any empty fields.
      jDoc = clean(jDoc);

      String jsonString = jDoc.toString(2);

      // Call the validator on the JSON if provided.
      String validationMessage = "OK";
      if (validator != null) {
        validationMessage = validate(jsonString);
      }

      ObjectMetadata s3Meta = new ObjectMetadata();
      s3Meta.setContentType("application/json");
      if (validationMessage.indexOf("OK") > -1) {
        String s3Key = s3Folder + "/" + collectionId + "_" + uuid.toString()
            + ".json";
        byte[] contentAsBytes = jsonString.getBytes("UTF-8");
        ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(
            contentAsBytes);
        s3Meta.setContentLength(contentAsBytes.length);
        s3.putObject(
            new PutObjectRequest(s3Bucket, s3Key, contentsAsStream, s3Meta));

        // If we parsed out a full_text_file, write that out.
        if (fullText != null) {
          if (fullText.length() > 0) {
            s3Meta.setContentType("text/plain");
            s3Key = s3Folder + "/" + collectionId + "_" + uuid.toString()
                + ".txt";
            contentAsBytes = fullText.getBytes("UTF-8");
            contentsAsStream = new ByteArrayInputStream(contentAsBytes);
            s3Meta.setContentLength(contentAsBytes.length);
            s3.putObject(new PutObjectRequest(s3Bucket, s3Key, contentsAsStream,
                s3Meta));
          } else {
            LOG.warn(
                "NutchField full_text_file is an empty String; Not writing to output");
          }
        }

      } else {
        // The JSON failed validation.
        LOG.error("UUID:" + uuid.toString() + ", " + validationMessage);
        if (s3InvalidFolder != null) {
          Object validationObj = (Object) validationMessage;
          if (validationMessage.startsWith("{")) {
            validationObj = new JSONObject(validationMessage);
          }
          jDoc.put("validation_message", validationObj);
          jsonString = jDoc.toString(2);
          String s3Key = s3InvalidFolder + "/" + collectionId + "-"
              + uuid.toString() + ".json";
          byte[] contentAsBytes = jsonString.getBytes("UTF-8");
          ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(
              contentAsBytes);
          s3Meta.setContentLength(contentAsBytes.length);
          s3.putObject(
              new PutObjectRequest(s3Bucket, s3Key, contentsAsStream, s3Meta));
        }
      }
    } catch (JSONException e1) {
      LOG.error(e1.getMessage(), e1);
    } catch (AmazonServiceException e) {
      LOG.error(e.getMessage(), e);
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
    }
  }

  /**
   * A utility to clean URLs of illegal characters. Based on Stack Overflow
   * conversation here
   * https://stackoverflow.com/questions/724043/http-url-address-encoding-in-java/4605816#4605816
   *
   * @param toEscape
   * @return the clean URL string
   */
  private String escapeIllegalUrlCharacters(String toEscape) {
    // If things go haywire, we return the original string.
    String newUrlStr = toEscape;

    try {
      URL url = new URL(toEscape);
      URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(),
          url.getPort(), url.getPath(), url.getQuery(), url.getRef());
      // if an escaped %XX is included in the toEscape string, it will be
      // re-encoded to %25 and we don't want re-encoding, just encoding
      URL newUrl = new URL(uri.toString().replace("%25", "%"));
      newUrlStr = newUrl.toString();
      newUrlStr = newUrlStr.replace(" ", "%20");
      if (!newUrlStr.equals(toEscape)) {
        LOG.info("Refactored " + toEscape + " to " + newUrlStr);
      }
    } catch (MalformedURLException mfe) {
      LOG.warn("Malformed URL Exception cleaning URL '" + toEscape + "', "
          + mfe.toString());
    } catch (URISyntaxException use) {
      LOG.warn("URI Syntax Exception cleaning URL '" + toEscape + "', "
          + use.toString());
    }

    return newUrlStr;
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
    properties.put(S3Constants.VALIDATOR,
        new AbstractMap.SimpleEntry<>("The validation endpoint",
            this.validator == null ? "" : this.validator));
    properties.put(S3Constants.INVALID_FOLDER,
        new AbstractMap.SimpleEntry<>(
            "The S3 folder to store invalid results in",
            this.s3InvalidFolder == null ? "" : this.s3InvalidFolder));
    properties.put(S3Constants.AWS_ACCESS_KEY_ID, new AbstractMap.SimpleEntry<>(
        "The AWS Access Key ID for accessing the bucket",
        this.awsAccessKeyId == null ? ""
            : "****"
                + this.awsAccessKeyId.substring(awsAccessKeyId.length() - 4)));
    properties.put(S3Constants.AWS_SECRET_ACCESS_KEY,
        new AbstractMap.SimpleEntry<>(
            "The AWS Secret Access Key credentials for accessing the bucket",
            this.awsSecretAccessKey == null ? ""
                : "****" + this.awsSecretAccessKey
                    .substring(awsSecretAccessKey.length() - 4)));
    properties.put(S3Constants.TEMPLATE,
        new AbstractMap.SimpleEntry<>(
            "The JSON template for the constructed doc",
            this.template == null ? "" : this.template));

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
