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
package org.apache.nutch.indexer.org;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An indexing plugin designed to add metadata about the domain hosting a seed url.
 * It will take the URL of the document, parse out the domain, and then
 * call an API to return metadata about that domain.  The fields returned by this
 * API will get added to the document metadata.
 */

public class OrgIndexer implements IndexingFilter {
  private static final Logger LOG = LoggerFactory
    .getLogger(MethodHandles.lookup().lookupClass());

  private LRUCache<String, HashMap<String, String>> cache =
    new LRUCache<String, HashMap<String, String>>(500);

  private HashMap<String, String> domainMap = new HashMap<String, String>();

  private Configuration conf;
  private String serviceUrl;
  private String xApiKey;

  /**
   * The {@link OrgIndexer} filter object which adds fields as per
   * configuration setting. See {@code index.org} in nutch-default.xml.
   * 
   * @param doc
   *          The {@link NutchDocument} object under construction
   * @param parse
   *          The relevant {@link Parse} object passing through the filter
   * @param url
   *          URL the document url corresponding to this NutchDocument
   * @param datum
   *          The {@link CrawlDatum} entry
   * @param inlinks
   *          The {@link Inlinks} containing anchor text
   * @return modified NutchDocument
   */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    // Find the domain of this seed URL
    try {
      String urlStr = url.toString();
      // Strip out any S3 storage prefix (applies to cases where we are 'crawling' archived content).
      urlStr = urlStr.replace("coherent-webarchive.s3.us-east-2.amazonaws.com/", "");
      URI uri = new URI(urlStr);
      String domain = uri.getHost();
      LOG.debug("URL is " + url.toString() + ", domain is " + domain);
      if (domain != null) {
        // Get the organization metadata for this domain
        Map<String, String> fields = getOrganizationMeta(domain);
        for (Entry<String, String> entry : fields.entrySet()) {
          doc.add(entry.getKey(), entry.getValue());
        }
      }
    } catch (URISyntaxException use) {
      LOG.error("Cannot parse domain from URL " + url.toString() + ", " + use.toString());
    }
    return doc;
  }

  /**
   * Populate a HashMap of organization metadata from the domain.
   * 
   * @param domain
   *          the host portion of the document url
   * @return HashMap of org fields and their corresponding values
   */
  private HashMap<String, String> getOrganizationMeta(String domain) {
    HashMap<String, String> fields = new HashMap<String, String>();

    /*
     * Pseudocode:
     * Search the domain in the cache or via index.org.serviceurl to obtain the org jsan.
     * Make a hashmap of fieldname = fieldvalue from the JSON.
     * Cache the hashmap for this org id in an LRU cache.
     * Return the hashmap.
     */

    // See if we have cached the fields for this domain
    HashMap<String, String> cachedFields = this.cache.get(domain);
    if (cachedFields != null) {
      return cachedFields;
    }

    // Call the org lookup API via domain or slug
    String query = domain;
    // In cases where the org cv does not have a searchable domain, 
    // we check a map of domain to slug.
    if (this.domainMap.get(domain) != null) {
      query = this.domainMap.get(domain);
    }

    URLConnection con = null;
    try {
      URL orgLookupUrl = new URL(this.serviceUrl + query);
      // Connect to org lookup service
      con = orgLookupUrl.openConnection();
      con.setConnectTimeout(5000);
      con.setReadTimeout(10000);
      con.addRequestProperty("x-api-key", this.xApiKey);
      con.connect();

      // Read the API response into a byte array
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      IOUtils.copy(con.getInputStream(), baos);
      baos.close();
      String jsonStr = baos.toString("UTF-8");
      if (jsonStr.length() == 0 || !jsonStr.startsWith("[")) {
        LOG.warn("Request for org information on " + domain + " returned unexpected response." + jsonStr);
        return fields;
      }
      JSONArray ja = new JSONArray(jsonStr);
      if (ja.length() == 0) {
        LOG.warn("Request for org information on " + domain + " returned no hits. Ignoring");
        return fields;
      }
      JSONObject jo = ja.getJSONObject(0);
      if (ja.length() > 1) {
	// The api will return multiple matches on a slug search if slug
	// is a substring of some other org.  Look for an exact match.
	for (int i = 0; i < ja.length(); i++) {
          JSONObject joTmp = ja.getJSONObject(i);
          String slug = joTmp.getString("slug");
	  if (query.equals(slug)) {
            jo = joTmp;
	    break;
	  } 
	}
      }

      if (jo.has("slug")) {
        fields.put("organization.id", jo.getString("slug"));
      }
      if (jo.has("name")) {
        fields.put("organization.name", jo.getString("name"));
      }
      if (jo.has("city") && !jo.getString("city").equals("null")) {
        fields.put("organization.city", jo.getString("city"));
      }
      if (jo.has("state") && !jo.getString("state").equals("null")) {
        fields.put("organization.region", jo.getString("state"));
      }
      if (jo.has("country_name") && !jo.getString("country_name").equals("null")) {
        fields.put("organization.country", jo.getString("country_name"));
      }
      if (jo.has("country_code") && !jo.getString("country_code").equals("null")) {
        fields.put("organization.country_code", jo.getString("country_code"));
      }
      if (jo.has("org_type") && !jo.getString("org_type").equals("null")) {
        fields.put("organization.type", jo.getString("org_type"));
      }
      this.cache.put(domain, fields);
    } catch (Exception e) {
      LOG.error("Unable to obtain Org metadata. " + e.toString());
    }

    return fields;
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;

    this.serviceUrl  = conf.getTrimmed("index.org.serviceurl");
    if (this.serviceUrl == null) {
      LOG.error("Please set index.org.serviceurl to use the index-org plugin");
    }

    this.xApiKey  = conf.getTrimmed("index.org.x-api-key");
    if (this.xApiKey == null) {
      LOG.error("Please set index.org.x-api-key to use the index-org plugin");
    }

    String domainMapFile  = conf.getTrimmed("index.org.domainmap");
    if (domainMapFile != null) {
       loadDomainMap(domainMapFile);
    }
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Read the domain map into a HashMap
   */
  public void loadDomainMap(String fileName) {
    BufferedReader reader = null;
    int rows = 0;
    try {
      reader = new BufferedReader(this.conf.getConfResourceAsReader(fileName));
      String line = reader.readLine();
      while (line != null) {
        if (line.trim().length() > 0) {
          String[] columns = line.trim().split("\t");
          // Domain is in column 1, org id is in column 3
          if (columns.length > 2) {
            this.domainMap.put(columns[0], columns[2]);
            rows++;
          }
        }
        line = reader.readLine();
      }
      reader.close();
    } catch (IOException e) {
      LOG.error("Unable to read domain map " + fileName + ", " + e.toString());
    }
    LOG.info("Loaded " + rows + " rows from the domain map " + fileName);
  }

  /**
   * An LRU cache, used to store organization metadata by domain.
   */
  public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private int cacheSize;

    public LRUCache(int cacheSize) {
      super(16, 0.75f, true);
      this.cacheSize = cacheSize;
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() >= cacheSize;
    }
  }
}
