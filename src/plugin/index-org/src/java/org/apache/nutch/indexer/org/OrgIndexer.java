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

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An indexing plugin designed to add metadata about the domain hosting a seed
 * url. It will take the URL of the document, parse out the domain, and then
 * call an API to return metadata about that domain. The fields returned by this
 * API will get added to the document metadata.
 */

public class OrgIndexer implements IndexingFilter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private LRUCache<String, HashMap<String, String>> cache = new LRUCache<String, HashMap<String, String>>(
      500);

  private Configuration conf;
  private String serviceUrl;
  private String xApiKey;

  /**
   * The {@link OrgIndexer} filter object which adds fields as per configuration
   * setting. See {@code index.org} in nutch-default.xml.
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
    String orgSlug = null;
    String domain = null;
    try {
      Writable metadata = datum.getMetaData().get(new Text("org.slug"));
      if (metadata != null) {
        orgSlug = metadata.toString();
      }

      try {
        String urlStr = url.toString();
        // Strip out any S3 storage prefix (applies to cases where we are
        // 'crawling' archived content).
        urlStr = urlStr
            .replace("coherent-webarchive.s3.us-east-2.amazonaws.com/", "");
        URI uri = new URI(urlStr);
        domain = uri.getHost();
        LOG.debug("URL is " + url.toString() + ", domain is " + domain);
      } catch (URISyntaxException use) {
        LOG.error("Cannot parse domain from URL " + url.toString() + ", "
            + use.toString());
      }

      if (orgSlug != null || domain != null) {
        // Get the organization metadata for this slug or domain
        Map<String, String> fields = getOrganizationMeta(orgSlug, domain);
        for (Entry<String, String> entry : fields.entrySet()) {
          doc.add(entry.getKey(), entry.getValue());
        }
      }
    } catch (Exception e) {
      LOG.error("Cannot obtain org metadata " + domain + ", org " + orgSlug,
          e.toString());
    }
    return doc;
  }

  /**
   * Populate a HashMap of organization metadata from the slug or domain.
   * 
   * @param orgSlug
   *          the organization id.
   * @param domain
   *          the host portion of the document url
   * @return HashMap of org fields and their corresponding values
   */
  private HashMap<String, String> getOrganizationMeta(String orgSlug,
      String domain) {
    HashMap<String, String> fields = new HashMap<String, String>();

    /*
     * Pseudocode: Search the slug in the cache or via index.org.serviceurl to
     * obtain the org json. Make a hashmap of fieldname = fieldvalue from the
     * JSON. Cache the hashmap for this org id in an LRU cache. Return the
     * hashmap. If slug is null, try the same thing by domain.
     */

    // Call the org lookup API via slug if provided, otherwise domain.
    String query = orgSlug;
    if (query == null) {
      query = domain;
    }

    // See if we have cached the fields for this organization
    HashMap<String, String> cachedFields = this.cache.get(query);
    if (cachedFields != null) {
      return cachedFields;
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
        LOG.warn("Request for org information on " + query
            + " returned unexpected response." + jsonStr);
        return fields;
      }
      JSONArray ja = new JSONArray(jsonStr);
      if (ja.length() == 0) {
        LOG.warn("Request for org information on " + query
            + " returned no hits. Ignoring");
        return fields;
      }
      // Default to the first hit.
      JSONObject jo = ja.getJSONObject(0);
      if (ja.length() > 1) {
        // The api can return multiple matches. Look for an exact match on the query.
        for (int i = 0; i < ja.length(); i++) {
          JSONObject joTmp = ja.getJSONObject(i);
          if (orgSlug != null) {
            String slug = joTmp.getString("slug");
            if (orgSlug.equals(slug)) {
              jo = joTmp;
              break;
            }
          } else if (domain != null ) {
            JSONArray domains = joTmp.getJSONArray("domains");
            for (int domIndex = 0; domIndex < domains.length(); domIndex++) {
              String d = domains.getString(domIndex);
              if (domain.equals(d)) {
                jo = joTmp;
                break;
              }
            }
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
      if (jo.has("country_name")
          && !jo.getString("country_name").equals("null")) {
        fields.put("organization.country", jo.getString("country_name"));
      }
      if (jo.has("country_code")
          && !jo.getString("country_code").equals("null")) {
        fields.put("organization.country_code", jo.getString("country_code"));
      }
      if (jo.has("org_type") && !jo.getString("org_type").equals("null")) {
        fields.put("organization.type", jo.getString("org_type"));
      }
      this.cache.put(query, fields);
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

    this.serviceUrl = conf.getTrimmed("index.org.serviceurl");
    if (this.serviceUrl == null) {
      LOG.error("Please set index.org.serviceurl to use the index-org plugin");
    }

    this.xApiKey = conf.getTrimmed("index.org.x-api-key");
    if (this.xApiKey == null) {
      LOG.error("Please set index.org.x-api-key to use the index-org plugin");
    }
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * An LRU cache, used to store organization metadata by slug or domain.
   */
  public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = 6429219277806426246L;
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
