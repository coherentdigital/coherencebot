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

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.FeedInjector;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Before;
import org.junit.Test;


/**
 * Test the Withdrawn Scoring Filter
 *
 * @author pciuffetti
 */
public class TestWithdrawnScoringFilter {

  public static final String SEED_KEY = "collection.seed";
  public static final Text SEED_KEY_W = new Text(SEED_KEY);

  Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();

    Path tempFile = Files.createTempFile(null, null);

    // Writes a string to the above temporary file
    Files.write(tempFile, "https://example.com\tcollection.seed=https://example.com\n".getBytes(StandardCharsets.UTF_8));

    conf.set(FeedInjector.FEED_INJECTOR_CRAWLPATH, tempFile.toAbsolutePath().toString());
    conf.setBoolean(CrawlDb.CRAWLDB_PURGE_WITHDRAWN, true);
  }

  @Test
  /**
   * This test creates a crawl datum with a collection.seed metadata
   * element.  In the first test the crawl datum is not marked withdrawn
   * @throws Exception
   */
  public void testAcceptedCrawlDatum() throws Exception {
    ScoringFilter filter = new WithdrawnScoringFilter();
    filter.setConf(conf);

    Text url = new Text("http://example.com/home");
    CrawlDatum datum = new CrawlDatum();
    datum.setStatus(CrawlDatum.STATUS_DB_NOTMODIFIED);
    // Set the collection seed of this datum to match the config file.
    MapWritable metadata = datum.getMetaData();
    metadata.put(SEED_KEY_W, new Text("https://example.com"));

    List<CrawlDatum> emptyListOfInlinks = new ArrayList<CrawlDatum>();

    // This URL should not be marked withdrawn
    filter.updateDbScore(url, datum, datum, emptyListOfInlinks);

    assertEquals(
        "Expected status db_notmodified but got "
            + CrawlDatum.getStatusName(datum.getStatus()),
        CrawlDatum.STATUS_DB_NOTMODIFIED, datum.getStatus());
  }

  @Test
  /**
   * This test creates a crawl dataum with a collection.seed metadata
   * element.  In this test the crawl datum is marked withdrawn
   * @throws Exception
   */
  public void testWithdrawnCrawlDatum() throws Exception {
    ScoringFilter filter = new WithdrawnScoringFilter();
    filter.setConf(conf);

    Text url = new Text("http://www.example.com/home");
    CrawlDatum datum = new CrawlDatum();
    datum.setStatus(CrawlDatum.STATUS_DB_NOTMODIFIED);
    // Set the collection seed of this datum to not match the config file.
    MapWritable metadata = datum.getMetaData();
    metadata.put(SEED_KEY_W, new Text("https://www.example.com"));

    List<CrawlDatum> emptyListOfInlinks = new ArrayList<CrawlDatum>();

    // This URL should be marked withdrawn
    filter.updateDbScore(url, datum, datum, emptyListOfInlinks);

    assertEquals(
        "Expected status db_withdrawn but got "
            + CrawlDatum.getStatusName(datum.getStatus()),
        CrawlDatum.STATUS_DB_WITHDRAWN, datum.getStatus());
  }
}
