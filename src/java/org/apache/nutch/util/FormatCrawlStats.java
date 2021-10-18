package org.apache.nutch.util;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormatCrawlStats extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  Map<String,JSONObject> hostsMeta = new HashMap<String,JSONObject>();

  public int createCrawlComplete() {
    // Read in seeds.txt which has all the collections this bot is working on.
    // Convert it to JSON in preparation for it being a host record.
    // Note: Nutch reports fetch/unfetched counts by host, not by collection.
    // So we produce only one dashboard record for each host.
    List<JSONObject> collectionObjs = new ArrayList<JSONObject>();
    try (Stream<String> stream = Files.lines(Path.of("seeds.txt"))) {
      // This turns each collection record in seeds.txt to a list of JSON objs.
      collectionObjs = stream
          .map(FormatCrawlStats::tsvToJson)
          .collect(Collectors.toList());
      LOG.info("Seeds.txt has {} collections", collectionObjs.size());

      // This loop collapses the seeds by host
      for (JSONObject collection : collectionObjs) {
        if (collection.has("host")) {
          String host = collection.getString("host");
          JSONObject hostObj = hostsMeta.get(host);
          if (hostObj == null) {
            hostsMeta.put(host, collection);
          } else {
            // Merge HOST objects
            JSONArray hostTitles = hostObj.getJSONArray("collection_titles");
            JSONArray orgTitles = collection.getJSONArray("collection_titles");
            if (orgTitles != null && hostTitles != null) {
              for (int i = 0; i< orgTitles.length(); i++) {
                String title = (String)orgTitles.get(i);
                hostTitles.put(title);
              }
            } else {
              LOG.error("No existing collection titles for {}", host);
            }
            JSONArray hostIds = hostObj.getJSONArray("collection_ids");
            JSONArray orgIds = collection.getJSONArray("collection_ids");
            if (orgIds != null && hostIds != null) {
              for (int i = 0; i< orgIds.length(); i++) {
                String id = (String)orgIds.get(i);
                hostIds.put(id);
              }
            } else {
              LOG.error("No existing collection ids for {}", host);
            }
            JSONArray hostSeeds = hostObj.getJSONArray("collection_seeds");
            JSONArray orgSeeds = collection.getJSONArray("collection_seeds");
            if (orgSeeds != null && hostSeeds != null) {
              for (int i = 0; i< orgSeeds.length(); i++) {
                String seed = (String)orgSeeds.get(i);
                hostSeeds.put(seed);
              }
            } else {
              LOG.error("No existing collection seeds for {}", host);
            }
            hostsMeta.put(host, hostObj);
          }
        }
      }
    } catch (Exception e) {
      LOG.error(e.toString());
    }

    // Read in the fetched/unfetched counts by host and update hostMeta.
    List<JSONObject> hostCounts = new ArrayList<JSONObject>();
    try (Stream<String> stream = Files.lines(Path.of("part-r-00000"))) {
      // This puts all the host counts for fetch/unfetched into a list of JSON objs.
      hostCounts = stream
          .map(FormatCrawlStats::ccToJson)
          .collect(Collectors.toList());
      for (JSONObject cc : hostCounts) {
        if (cc.has("host")) {
          String host = cc.getString("host");
          JSONObject hostMeta = hostsMeta.get(host);
          if (hostMeta == null) {
            LOG.warn("No collection info in seeds.txt for host {}", host);
            // The nutch database is reporting data for a host not in the seed list.
            // Make in an dashboard record with unknown values for the collection meta.
            hostMeta = new JSONObject();
            hostMeta.put("host", host);
            hostMeta.put("id", host);
            hostMeta.put("collection_ids", "unknown");
            hostMeta.put("collection_titles", "unknown");
            hostMeta.put("collection_seeds", "unknown");
            hostMeta.put("org_slug", "unknown");
            hostMeta.put("type", "host");
          }
          if (cc.has("unfetched")) {
            hostMeta.put("unfetched", cc.get("unfetched"));
          }
          if (cc.has("fetched")) {
            hostMeta.put("fetched", cc.get("fetched"));
          }
          hostsMeta.put(host, hostMeta);
        } else {
          LOG.error("No host field for {}", cc.toString());
        }
      }
    } catch (IOException ioe) {
      LOG.error(ioe.toString());
    } catch (JSONException je) {
      LOG.error(je.toString());
    }

    // Ok, we have all the hosts and we've merged in the fetched/unfetched counts
    // Output the JSON to a file we can index.
    JSONArray hostsMetaJson = new JSONArray();
    LOG.info("Hosts meta has {} entries", hostsMeta.size());
    for (String host : hostsMeta.keySet()) {
      JSONObject hostObj = hostsMeta.get(host);
      hostsMetaJson.put(hostObj);
    }


    try {
      LOG.info("Writing hosts.json");
      FileWriter jsonWriter = new FileWriter("hosts.json");
      hostsMetaJson.write(jsonWriter);
      jsonWriter.flush();
      jsonWriter.close();
    } catch (IOException ioe) {
      LOG.error(ioe.toString());
    } catch (JSONException je) {
      LOG.error(je.toString());
    }
    
    return 0;
  }

  private static JSONObject tsvToJson(String line) {
    if (line != null && line.length() > 0) {
      String columns[] = line.split("\t");
      if (columns.length >= 5) {
        try {
          JSONObject jo = new JSONObject();
          jo.put("url", columns[0]);
          URI uri = new URI(columns[0]);
          jo.put("host", uri.getHost());
          jo.put("id", uri.getHost());
          jo.put("collection_titles", new JSONArray("[\"" + columns[1].split("=")[1] + "\"]"));
          jo.put("collection_ids", new JSONArray("[\"" + columns[2].split("=")[1] + "\"]"));
          jo.put("collection_seeds", new JSONArray("[\"" + columns[3].split("=")[1] + "\"]"));
          jo.put("org_slug", columns[4].split("=")[1]);
          jo.put("type", "host");
          return jo;
        } catch (JSONException je) {
          LOG.error(je.toString());
        } catch (URISyntaxException use) {
          LOG.error(use.toString());
        }
      }
    }
    return null;
  }

  /**
   * Parse a line from the CrawlComplete output into JSON
   * @param line looks like "COUNT\tHOST UNFETCHED|FETCHED"
   * @return
   */
  private static JSONObject ccToJson(String line) {
    if (line != null && line.length() > 0) {
      String columns[] = line.split("\t");
      if (columns.length == 2) {
        try {
          JSONObject jo = new JSONObject();
          String parts[] = columns[1].split(" ");
          if (parts.length == 2) {
            if ("UNFETCHED".equals(parts[1])) {
              jo.put("unfetched", Integer.parseInt(columns[0]));
            }
            if ("FETCHED".equals(parts[1])) {
              jo.put("fetched", Integer.parseInt(columns[0]));
            }
            jo.put("host", parts[0]);
            return jo;
          }
        } catch (JSONException je) {
          LOG.error(je.toString());
        }
      }
    }
    return null;
  }

  @Override
  public int run(String[] arg0) throws Exception {
    return createCrawlComplete();
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(NutchConfiguration.create(), new FormatCrawlStats(), args);
  }
}