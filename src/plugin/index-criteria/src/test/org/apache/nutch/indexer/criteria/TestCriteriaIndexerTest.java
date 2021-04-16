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
package org.apache.nutch.indexer.criteria;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for Criteria Indexing
 * 
 * @author pciuffetti
 */

public class TestCriteriaIndexerTest {

  Configuration conf;

  Outlink[] outlinks = new Outlink[5];
  Inlinks inlinks;
  ParseImpl parse;
  CrawlDatum crawlDatum;
  Text url;
  CriteriaIndexer filter;

  @Before
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    parse = new ParseImpl("", new ParseData(new ParseStatus(), "title", outlinks, new Metadata()));
    url = new Text("http://nutch.apache.org/index.html");
    crawlDatum = new CrawlDatum();
    inlinks = new Inlinks();
    filter = new CriteriaIndexer();
  }

  /**
   * Test that a zero content length situation gets filtered out
   * 
   * @throws Exception
   */
  @Test
  public void testEmptyDocument() throws Exception {

    Assert.assertNotNull(filter);
    filter.setConf(conf);

    NutchDocument doc = new NutchDocument();

    try {
      filter.filter(doc, parse, url, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    Assert.assertNotNull(doc);
    Assert.assertTrue("tests if no field is set for empty index.criteria", doc
        .getFieldNames().isEmpty());
  }
}
