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
package org.apache.nutch.crawl;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * FeedInjector takes an API feed of URLs and merges ("injects") these URLs into
 * the CrawlDb. Used for bootstrapping or inserting new work into a Nutch crawl.
 * 
 **/
public class FeedInjector extends NutchTool implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * property to pass value of command-line option -filterNormalizeAll to mapper
   */
  public static final String URL_FILTER_NORMALIZE_ALL = "crawldb.inject.filter.normalize.all";

  /** metadata key reserved for setting a custom score for a specific URL */
  public static String nutchScoreMDName = "nutch.score";

  /** properties for connecting to the feed endpoint */
  public static final String FEED_INJECTOR_ENDPOINT = "crawldb.inject.feed.endpoint";
  public static final String FEED_INJECTOR_PARAMS = "crawldb.inject.feed.params";
  public static final String FEED_INJECTOR_APIKEY = "crawldb.inject.feed.x-api-key";
  public static final String FEED_INJECTOR_METADATA_PROPS = "crawldb.inject.feed.md";

  /**
   * metadata key reserved for setting a custom fetchInterval for a specific URL
   */
  public static String nutchFetchIntervalMDName = "nutch.fetchInterval";

  /**
   * metadata key reserved for setting a fixed custom fetchInterval for a
   * specific URL
   */
  public static String nutchFixedFetchIntervalMDName = "nutch.fetchInterval.fixed";

  /**
   * InjectMapper reads
   * <ul>
   * <li>the CrawlDb seeds are injected into</li>
   * <li>the plain-text seed files and parses each line into the URL and
   * metadata. Seed URLs are passed to the reducer with STATUS_INJECTED.</li>
   * </ul>
   * Depending on configuration and command-line parameters the URLs are
   * normalized and filtered using the configured plugins.
   */
  public static class FeedInjectMapper
      extends Mapper<Text, Writable, Text, CrawlDatum> {
    public static final String URL_NORMALIZING_SCOPE = "crawldb.url.normalizers.scope";
    public static final String TAB_CHARACTER = "\t";
    public static final String EQUAL_CHARACTER = "=";

    private URLNormalizers urlNormalizers;
    private int interval;
    private float scoreInjected;
    private URLFilters filters;
    private ScoringFilters scfilters;
    private long curTime;
    private boolean url404Purging;
    private String scope;
    private boolean filterNormalizeAll = false;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      boolean normalize = conf.getBoolean(CrawlDbFilter.URL_NORMALIZING, true);
      boolean filter = conf.getBoolean(CrawlDbFilter.URL_FILTERING, true);
      filterNormalizeAll = conf.getBoolean(URL_FILTER_NORMALIZE_ALL, false);
      if (normalize) {
        scope = conf.get(URL_NORMALIZING_SCOPE, URLNormalizers.SCOPE_INJECT);
        urlNormalizers = new URLNormalizers(conf, scope);
      }
      interval = conf.getInt("db.fetch.interval.default", 2592000);
      if (filter) {
        filters = new URLFilters(conf);
      }
      scfilters = new ScoringFilters(conf);
      scoreInjected = conf.getFloat("db.score.injected", 1.0f);
      curTime = conf.getLong("injector.current.time",
          System.currentTimeMillis());
      url404Purging = conf.getBoolean(CrawlDb.CRAWLDB_PURGE_404, false);
    }

    /* Filter and normalize the input url */
    private String filterNormalize(String url) {
      if (url != null) {
        try {
          if (urlNormalizers != null)
            url = urlNormalizers.normalize(url, scope); // normalize the url
          if (filters != null)
            url = filters.filter(url); // filter the url
        } catch (Exception e) {
          LOG.warn("Skipping " + url + ":" + e);
          url = null;
        }
      }
      return url;
    }

    /**
     * Extract metadata that could be passed along with url in a seeds file.
     * Metadata must be key-value pair(s) and separated by a TAB_CHARACTER
     */
    private void processMetaData(String metadata, CrawlDatum datum,
        String url) {
      String[] splits = metadata.split(TAB_CHARACTER);

      for (String split : splits) {
        // find separation between name and value
        int indexEquals = split.indexOf(EQUAL_CHARACTER);
        if (indexEquals == -1) // skip anything without an = (EQUAL_CHARACTER)
          continue;

        String metaname = split.substring(0, indexEquals);
        String metavalue = split.substring(indexEquals + 1);

        try {
          if (metaname.equals(nutchScoreMDName)) {
            datum.setScore(Float.parseFloat(metavalue));
          } else if (metaname.equals(nutchFetchIntervalMDName)) {
            datum.setFetchInterval(Integer.parseInt(metavalue));
          } else if (metaname.equals(nutchFixedFetchIntervalMDName)) {
            int fixedInterval = Integer.parseInt(metavalue);
            if (fixedInterval > -1) {
              // Set writable using float. Float is used by
              // AdaptiveFetchSchedule
              datum.getMetaData().put(Nutch.WRITABLE_FIXED_INTERVAL_KEY,
                  new FloatWritable(fixedInterval));
              datum.setFetchInterval(fixedInterval);
            }
          } else {
            datum.getMetaData().put(new Text(metaname), new Text(metavalue));
          }
        } catch (NumberFormatException nfe) {
          LOG.error("Invalid number '" + metavalue + "' in metadata '"
              + metaname + "' for url " + url);
        }
      }
    }

    @Override
    public void map(Text key, Writable value, Context context)
        throws IOException, InterruptedException {
      if (value instanceof Text) {
        // if its a url from the seed list
        String url = key.toString().trim();

        // remove empty string or string starting with '#'
        if (url.length() == 0 || url.startsWith("#"))
          return;

        url = filterNormalize(url);
        if (url == null) {
          context.getCounter("injector", "urls_filtered").increment(1);
        } else {
          CrawlDatum datum = new CrawlDatum();
          datum.setStatus(CrawlDatum.STATUS_INJECTED);
          datum.setFetchTime(curTime);
          datum.setScore(scoreInjected);
          datum.setFetchInterval(interval);

          String metadata = value.toString().trim();
          if (metadata.length() > 0)
            processMetaData(metadata, datum, url);

          try {
            key.set(url);
            scfilters.injectedScore(key, datum);
          } catch (ScoringFilterException e) {
            LOG.warn(
                "Cannot filter injected score for url {}, using default ({})",
                url, e.getMessage());
          }
          context.getCounter("injector", "urls_injected").increment(1);
          context.write(key, datum);
        }
      } else if (value instanceof CrawlDatum) {
        // if its a crawlDatum from the input crawldb, emulate CrawlDbFilter's
        // map()
        CrawlDatum datum = (CrawlDatum) value;

        // remove 404 urls
        if (url404Purging && CrawlDatum.STATUS_DB_GONE == datum.getStatus()) {
          context.getCounter("injector", "urls_purged_404").increment(1);
          return;
        }

        if (filterNormalizeAll) {
          String url = filterNormalize(key.toString());
          if (url == null) {
            context.getCounter("injector", "urls_purged_filter").increment(1);
          } else {
            key.set(url);
            context.write(key, datum);
          }
        } else {
          context.write(key, datum);
        }
      }
    }
  }

  /** Combine multiple new entries for a url. */
  public static class FeedInjectReducer
      extends Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private int interval;
    private float scoreInjected;
    private boolean overwrite = false;
    private boolean update = false;
    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum injected = new CrawlDatum();

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      interval = conf.getInt("db.fetch.interval.default", 2592000);
      scoreInjected = conf.getFloat("db.score.injected", 1.0f);
      overwrite = conf.getBoolean("db.injector.overwrite", false);
      update = conf.getBoolean("db.injector.update", false);
      LOG.info("Injector: overwrite: " + overwrite);
      LOG.info("Injector: update: " + update);
    }

    /**
     * Merge the input records of one URL as per rules below :
     * 
     * <pre>
     * 1. If there is ONLY new injected record ==&gt; emit injected record
     * 2. If there is ONLY old record          ==&gt; emit existing record
     * 3. If BOTH new and old records are present:
     *    (a) If 'overwrite' is true           ==&gt; emit injected record
     *    (b) If 'overwrite' is false :
     *        (i)  If 'update' is false        ==&gt; emit existing record
     *        (ii) If 'update' is true         ==&gt; update existing record and emit it
     * </pre>
     * 
     * For more details @see NUTCH-1405
     */
    @Override
    public void reduce(Text key, Iterable<CrawlDatum> values, Context context)
        throws IOException, InterruptedException {

      boolean oldSet = false;
      boolean injectedSet = false;

      // If we encounter a datum with status as STATUS_INJECTED, then its a
      // newly injected record. All other statuses correspond to an old record.
      for (CrawlDatum val : values) {
        if (val.getStatus() == CrawlDatum.STATUS_INJECTED) {
          injected.set(val);
          injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
          injectedSet = true;
        } else {
          old.set(val);
          oldSet = true;
        }
      }

      CrawlDatum result;
      if (injectedSet && (!oldSet || overwrite)) {
        // corresponds to rules (1) and (3.a) in the method description
        result = injected;
      } else {
        // corresponds to rules (2) and (3.b) in the method description
        result = old;

        if (injectedSet && update) {
          // corresponds to rule (3.b.ii) in the method description
          old.putAllMetaData(injected);
          old.setScore(
              injected.getScore() != scoreInjected ? injected.getScore()
                  : old.getScore());
          old.setFetchInterval(injected.getFetchInterval() != interval
              ? injected.getFetchInterval()
              : old.getFetchInterval());
        }
      }
      if (injectedSet && oldSet) {
        context.getCounter("injector", "urls_merged").increment(1);
      }
      context.write(key, result);
    }
  }

  public FeedInjector() {
  }

  public FeedInjector(Configuration conf) {
    setConf(conf);
  }

  public void inject(Path crawlDb)
      throws IOException, ClassNotFoundException, InterruptedException {
    inject(crawlDb, false, false);
  }

  public void inject(Path crawlDb, boolean overwrite, boolean update)
      throws IOException, ClassNotFoundException, InterruptedException {
    inject(crawlDb, overwrite, update, true, true, false);
  }

  public void inject(Path crawlDb, boolean overwrite, boolean update,
      boolean normalize, boolean filter, boolean filterNormalizeAll)
      throws IOException, ClassNotFoundException, InterruptedException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    Path seedPath = null;

    LOG.info("FeedInjector: starting at {}", sdf.format(start));
    LOG.info("FeedInjector: crawlDb: {}", crawlDb);
    Configuration conf = getConf();

    // At this point we contact our feed and make a URL file to send to
    // Map/Reduce
    LOG.info("FeedInjector: Contacting feed for seed urls to crawl.");
    String feedEndpoint = conf.getTrimmed(FEED_INJECTOR_ENDPOINT);
    String feedParams = conf.getTrimmed(FEED_INJECTOR_PARAMS);
    String feedApiKey = conf.getTrimmed(FEED_INJECTOR_APIKEY);
    // String[] feedMetaProps = conf.getStrings(FEED_INJECTOR_METADATA_PROPS);
    URL seedFeedUrl = null;

    try {
      seedFeedUrl = new URL(feedEndpoint + '?' + feedParams);
    } catch (MalformedURLException e) {
      LOG.error(e.toString());
      return;
    }

    URLConnection con = null;
    List<String> seedUrls = new ArrayList<String>();
    try {
      while (seedFeedUrl != null) {
        // Connect to commons collection lookup service
        con = seedFeedUrl.openConnection();
        con.setConnectTimeout(5000);
        con.setReadTimeout(10000);
        con.addRequestProperty("x-api-key", feedApiKey);
        con.connect();

        // Read the API response into a byte array.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(con.getInputStream(), baos);
        baos.close();
        String jsonStr = baos.toString("UTF-8");
        if (jsonStr.length() == 0 || !jsonStr.startsWith("{")) {
          LOG.error(
              "Request for collection information returned unexpected response."
                  + jsonStr);
          break;
        }
        JSONObject jo = new JSONObject(jsonStr);
        if (!jo.has("count")) {
          LOG.error(
              "Request for collection information returned no count for request "
                  + seedFeedUrl.toString());
        }

        // Process these results.
        if (jo.has("results")) {
          JSONArray ja = jo.getJSONArray("results");
          if (ja.length() > 0) {
            for (int i = 0; i < ja.length(); i++) {
              String seedUrl = null;
              String collectionId = null;
              String collectionTitle = null;
              String collectionOrgId = null;
              JSONObject collection = ja.getJSONObject(i);
              if (collection.has("uuid")) {
                collectionId = collection.getString("uuid");
              }
              if (collection.has("url")) {
                seedUrl = collection.getString("url");
              }
              if (collection.has("title")) {
                collectionTitle = collection.getString("title");
              }
              if (collection.has("org")) {
                JSONObject org = collection.getJSONObject("org");
                if (org.has("slug")) {
                  collectionOrgId = org.getString("slug");
                }
              }
              if (seedUrl != null && collectionId != null
                  && collectionTitle != null && collectionOrgId != null) {
                // We have all the metadata
                String seedFileLine = seedUrl + "\tcollection.title=" + collectionTitle
                    + "\tcollection.id=" + collectionId + "\torg.slug="
                    + collectionOrgId;
                seedUrls.add(seedFileLine);
              } else {
                LOG.warn("Row " + i + " in " + seedFeedUrl.toString()
                    + " is incomplete; ignoring.");
              }
            }
          }
        } else {
          LOG.warn("empty results for request " + seedFeedUrl.toString());
        }

        // Setup for the next page of results.
        seedFeedUrl = null;
        if (jo.has("next") && !jo.isNull("next")) {
          try {
            seedFeedUrl = new URL(jo.getString("next"));
          } catch (MalformedURLException mue) {
            LOG.error(mue.toString());
          }
        }
      } // End while loop through pages of API results.

      // We have all the Seed URLs from the API; write to a file for map/reduce
      if (seedUrls.size() > 0) {
        String slug = feedParams.replace('&', '-').replace('=', '-');
        seedPath = new Path(crawlDb.getParent(),
            "seeds-" + slug + ".txt");
        FileSystem fileSystem = crawlDb.getFileSystem(conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(seedPath, true);
        BufferedWriter bufferedWriter = new BufferedWriter(
            new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        for (String line : seedUrls) {
          bufferedWriter.write(line);
          bufferedWriter.newLine();
        }
        bufferedWriter.close();
        fileSystem.close();
      } else {
        LOG.error("No seed URLs returned from the API " + seedFeedUrl.toString());
        return;
      }
    } catch (Exception e) {
      LOG.error("Unable to obtain collection metadata. " + e.toString());
      return;
    }

    LOG.info("FeedInjector: Converting injected urls to crawl db entries.");
    // set configuration for injection
    conf.setLong("injector.current.time", System.currentTimeMillis());
    conf.setBoolean("db.injector.overwrite", overwrite);
    conf.setBoolean("db.injector.update", update);
    conf.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);
    conf.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
    conf.setBoolean(URL_FILTER_NORMALIZE_ALL, filterNormalizeAll);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    // create all the required paths
    FileSystem fs = crawlDb.getFileSystem(conf);
    Path current = new Path(crawlDb, CrawlDb.CURRENT_NAME);
    if (!fs.exists(current))
      fs.mkdirs(current);

    Path tempCrawlDb = new Path(crawlDb,
        "crawldb-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // lock an existing crawldb to prevent multiple simultaneous updates
    Path lock = CrawlDb.lock(conf, crawlDb, false);

    // configure job
    Job job = Job.getInstance(conf, "injecting feed ");
    job.setJarByClass(FeedInjector.class);
    job.setMapperClass(FeedInjectMapper.class);
    job.setReducerClass(FeedInjectReducer.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setSpeculativeExecution(false);

    // set input and output paths of the job
    MultipleInputs.addInputPath(job, current, SequenceFileInputFormat.class);
    FileStatus[] seedFiles = seedPath.getFileSystem(getConf())
        .listStatus(seedPath);
    int numSeedFiles = 0;
    for (FileStatus seedFile : seedFiles) {
      if (seedFile.isFile()) {
        numSeedFiles++;
        LOG.info("Injecting seed URL file {}", seedPath);
        MultipleInputs.addInputPath(job, seedPath,
            KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job, tempCrawlDb);
      }
    }
    if (numSeedFiles == 0) {
      LOG.error("No seed files to inject found in {}", seedPath);
      LockUtil.removeLockFile(fs, lock);
      return;
    }

    try {
      // run the job
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "FeedInjector job did not succeed, job status: "
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        NutchJob.cleanupAfterFailure(tempCrawlDb, lock, fs);
        // throw exception so that calling routine can exit with error
        throw new RuntimeException(message);
      }

      // save output and perform cleanup
      CrawlDb.install(job, crawlDb);

      if (LOG.isInfoEnabled()) {
        long urlsInjected = job.getCounters()
            .findCounter("injector", "urls_injected").getValue();
        long urlsFiltered = job.getCounters()
            .findCounter("injector", "urls_filtered").getValue();
        long urlsMerged = job.getCounters()
            .findCounter("injector", "urls_merged").getValue();
        long urlsPurged404 = job.getCounters()
            .findCounter("injector", "urls_purged_404").getValue();
        long urlsPurgedFilter = job.getCounters()
            .findCounter("injector", "urls_purged_filter").getValue();
        LOG.info(
            "FeedInjector: Total urls rejected by filters: " + urlsFiltered);
        LOG.info(
            "FeedInjector: Total urls injected after normalization and filtering: "
                + urlsInjected);
        LOG.info("FeedInjector: Total urls injected but already in CrawlDb: "
            + urlsMerged);
        LOG.info("FeedInjector: Total new urls injected: "
            + (urlsInjected - urlsMerged));
        if (filterNormalizeAll) {
          LOG.info(
              "FeedInjector: Total urls removed from CrawlDb by filters: {}",
              urlsPurgedFilter);
        }
        if (conf.getBoolean(CrawlDb.CRAWLDB_PURGE_404, false)) {
          LOG.info(
              "FeedInjector: Total urls with status gone removed from CrawlDb (db.update.purge.404): {}",
              urlsPurged404);
        }

        long end = System.currentTimeMillis();
        LOG.info("FeedInjector: finished at " + sdf.format(end) + ", elapsed: "
            + TimingUtil.elapsedTime(start, end));
      }
    } catch (IOException | InterruptedException | ClassNotFoundException
        | NullPointerException e) {
      LOG.error("FeedInjector job failed: {}", e.getMessage());
      NutchJob.cleanupAfterFailure(tempCrawlDb, lock, fs);
      throw e;
    }
  }

  public void usage() {
    System.err.println(
        "Usage: FeedInjector [-D...] <crawldb>  [-overwrite|-update] [-noFilter] [-noNormalize] [-filterNormalizeAll]\n");
    System.err.println(
        "  <crawldb>\tPath to a crawldb directory. If not present, a new one would be created.");
    System.err.println(
        " -overwrite\tOverwite existing crawldb records by the injected records. Has precedence over 'update'");
    System.err.println(
        " -update   \tUpdate existing crawldb records with the injected records. Old metadata is preserved");
    System.err.println();
    System.err.println(" -nonormalize\tDo not normalize URLs before injecting");
    System.err
        .println(" -nofilter \tDo not apply URL filters to injected URLs");
    System.err.println(" -filterNormalizeAll\n"
        + "           \tNormalize and filter all URLs including the URLs of existing CrawlDb records");
    System.err.println();
    System.err.println(
        " -D...     \tset or overwrite configuration property (property=value)");
    System.err.println(" -Ddb.update.purge.404=true\n"
        + "           \tremove URLs with status gone (404) from CrawlDb");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new FeedInjector(),
        args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      usage();
      return -1;
    }

    boolean overwrite = false;
    boolean update = false;
    boolean normalize = true;
    boolean filter = true;
    boolean filterNormalizeAll = false;

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-overwrite")) {
        overwrite = true;
      } else if (args[i].equals("-update")) {
        update = true;
      } else if (args[i].equals("-noNormalize")) {
        normalize = false;
      } else if (args[i].equals("-noFilter")) {
        filter = false;
      } else if (args[i].equals("-filterNormalizeAll")) {
        filterNormalizeAll = true;
      } else {
        LOG.info("Injector: Found invalid argument \"" + args[i] + "\"\n");
        usage();
        return -1;
      }
    }

    try {
      inject(new Path(args[0]), overwrite, update, normalize, filter,
          filterNormalizeAll);
      return 0;
    } catch (Exception e) {
      LOG.error("FeedInjector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  /**
   * Used by the Nutch REST service
   */
  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId)
      throws Exception {
    if (args.size() < 1) {
      throw new IllegalArgumentException(
          "Required arguments <url_dir> or <seedName>");
    }
    Map<String, Object> results = new HashMap<>();
    Path crawlDb;
    if (args.containsKey(Nutch.ARG_CRAWLDB)) {
      Object crawldbPath = args.get(Nutch.ARG_CRAWLDB);
      if (crawldbPath instanceof Path) {
        crawlDb = (Path) crawldbPath;
      } else {
        crawlDb = new Path(crawldbPath.toString());
      }
    } else {
      crawlDb = new Path(crawlId + "/crawldb");
    }
    inject(crawlDb);
    results.put(Nutch.VAL_RESULT, Integer.toString(0));
    return results;
  }
}
