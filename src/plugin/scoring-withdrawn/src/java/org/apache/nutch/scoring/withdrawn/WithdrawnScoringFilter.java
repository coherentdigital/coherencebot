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
package org.apache.nutch.scoring.withdrawn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.FeedInjector;
import org.apache.nutch.scoring.AbstractScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scoring filter that determines whether a page has become withdrawn,
 * e.g. it's seed (as saved in the db metadata by the FeedInjector)
 * is no longer present in the list of seeds this bot is responsible for.
 * If the seed for a page isn't in the feed,
 * the page is marked for removal from the CrawlDB.
 */
public class WithdrawnScoringFilter extends AbstractScoringFilter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String SEED_KEY = "collection.seed";
  public static final Text SEED_KEY_W = new Text(SEED_KEY);
  private static Set<String> SEEDS = new HashSet<String>();
  public static final String TAB_CHARACTER = "\t";
  public static final String EQUAL_CHARACTER = "=";

  public void setConf(Configuration conf) {
    // We have to compute the location of the file containing the seed list.
    // In a EMR setup, this is an S3 file in the bot's crawldb.
    // In a local crawl, the crawlpath needs to contain the local file name (e.g. test-url.txt)
    String seedPath = null;
    String feedPath = conf.getTrimmed(FeedInjector.FEED_INJECTOR_CRAWLPATH);
    if (feedPath != null && feedPath.length() > 0) {
      if (feedPath.indexOf("s3://") == 0) {
        String feedParams = conf.getTrimmed(FeedInjector.FEED_INJECTOR_PARAMS, "");
        String slug = feedParams.replace('&', '-').replace('=', '-');
        seedPath = feedPath + "/crawl/" + "seeds-" + slug + ".txt";
      } else {
        seedPath = feedPath;
      }
      Path crawlDB = new Path(seedPath);
      BufferedReader br = null;
      try {
        try {
          FileSystem fs = crawlDB.getFileSystem(conf);
          br = new BufferedReader(new InputStreamReader(fs.open(crawlDB)));
        } catch (UnsupportedFileSystemException ufse) {
            LOG.info("Cant read {}", seedPath);
        }
        String line;
        SEEDS.clear();
        while ((line = br.readLine()) != null) {
          if (line.length() == 0)
            continue;

          char first = line.charAt(0);
          switch (first) {
          case ' ':
          case '\n':
          case '#': // skip blank & comment lines
            continue;
          default:
            String[] splits = line.split(TAB_CHARACTER);

            for (String split : splits) {
              // find separation between name and value
              int indexEquals = split.indexOf(EQUAL_CHARACTER);
              if (indexEquals == -1) // skip anything without an = (EQUAL_CHARACTER)
                continue;

              String metaname = split.substring(0, indexEquals);
              String metavalue = split.substring(indexEquals + 1);

              if (metaname.equals("collection.seed")) {
                SEEDS.add(metavalue);
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Error reading seed file " + seedPath, e.toString());
      } finally {
        // Close out the BufferedReader
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            // Ignore
          }
        }
        LOG.info("Read {} seeds from {}", SEEDS.size(), seedPath);
      }
    } else {
      LOG.info("No seed list configured at {}", FeedInjector.FEED_INJECTOR_CRAWLPATH);
    }
  }

  /**
   * Used for withdrawn status update.
   *
   * @param url
   *          of the record
   * @param old
   *          CrawlDatum
   * @param datum
   *          new CrawlDatum
   * @param inlinks
   *          list of inlinked CrawlDatums
   */
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinks) throws ScoringFilterException {

    // Do we have a list of seeds?
    if (SEEDS.size() > 0) {
      // Are we responsible for the seed in this record?
      if (old != null) {
        Text oldSeed = (Text) old.getMetaData().get(SEED_KEY_W);
        if (oldSeed != null) {
          String oldSeedStr = oldSeed.toString();
          if (!SEEDS.contains(oldSeedStr)) {
            datum.setStatus(CrawlDatum.STATUS_DB_WITHDRAWN);
            LOG.info("Withdrawing URL {}, seed {} not in service", url.toString(), oldSeedStr);
          }
        }
      }
    }
  }
}
