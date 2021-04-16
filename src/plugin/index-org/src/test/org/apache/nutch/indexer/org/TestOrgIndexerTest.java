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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test cases for the index.org plugin
 */

public class TestOrgIndexerTest {

  Configuration conf;

  Inlinks inlinks;
  ParseImpl parse;
  CrawlDatum crawlDatum;
  Text url;
  OrgIndexer filter;

  @Before
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    parse = new ParseImpl();
    url = new Text("https://coherent-webarchive.s3.us-east-2.amazonaws.com/acpp.info/PDFs/Denali_KidCare.pdf");
    crawlDatum = new CrawlDatum();
    inlinks = new Inlinks();
    filter = new OrgIndexer();
    conf.set("index.org.serviceurl","https://commons.coherentdigital.net/api/orgs/all/autocomplete/private/?format=json&q=");
    conf.set("index.org.x-api-key","pv=f=-q930xvsfuf(z@o-*ha^!sm2l7vncau_lgr@6m)k$voxk");
  }

  /**
   * Test an org lookup for domain accp.info
   * 
   * @throws Exception
   */
  @Test
  public void testNormalScenario() throws Exception {

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
    Assert.assertFalse("test if doc is not empty", doc.getFieldNames()
        .isEmpty());
    Assert.assertEquals("test if doc has 7 fields", 7, doc.getFieldNames()
        .size());
    Assert.assertTrue("test if doc has organization.id", doc.getField("organization.id")
        .getValues().contains("alaska-center-for-public-policy-us"));
    Assert.assertTrue("test if doc has organization.name", doc.getField("organization.name")
        .getValues().contains("Alaska Center for Public Policy"));
    Assert.assertTrue("test if doc has organization.country", doc.getField("organization.country")
        .getValues().contains("United States of America"));
    Assert.assertTrue("test if doc has organization.country_code", doc.getField("organization.country_code")
        .getValues().contains("US"));
    Assert.assertTrue("test if doc has organization.region", doc.getField("organization.region")
        .getValues().contains("AK"));
    Assert.assertTrue("test if doc has organization.city", doc.getField("organization.city")
        .getValues().contains("Anchorage"));
    Assert.assertTrue("test if doc has organization.type", doc.getField("organization.type")
        .getValues().contains("NGO"));
  }

}
