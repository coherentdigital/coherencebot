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
package org.apache.nutch.analysis;

import java.lang.invoke.MethodHandles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import junit.framework.TestCase;

public class TestArchiveParseFilter extends TestCase {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  // We have a config here that rejects an Anchor = reject
  private ArchiveParseFilter parseFilter;
  private Configuration conf;
  private AmazonS3 s3Client;
  private String  awsAccessKeyId;
  private String  awsSecretKey;
  private static DefaultAWSCredentialsProviderChain dacpc =  new DefaultAWSCredentialsProviderChain();
  protected String fileSeparator = System.getProperty("file.separator");
  /**
   * Folder with test data, defined in src/plugin/build-plugin.xml. Make sure
   * that all sample files are copied to "test.data", they must be listed in
   * src/plugin/parse-tika/build.xml
   */
  protected String sampleDir = System.getProperty("test.data", ".");

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    AWSCredentials credentials = dacpc.getCredentials();
    awsAccessKeyId =  credentials.getAWSAccessKeyId();
    awsSecretKey =  credentials.getAWSSecretKey();

    s3Client = AmazonS3ClientBuilder.standard()
        .withRegion("us-east-2")
        .build();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * @throws Exception
   */
  public void testArchiveParseFilterConf() throws Exception {
    // Check that the config has the credentials.
    LOG.info("Testing ArchiveParseFilter");
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", "protocol-file|parse-tika|parsefilter-archive");
    conf.set(
        "parsefilter.archive.s3bucket",
        "coherent-commons-digital-assets-dev"
    );
    conf.set(
        "parsefilter.archive.s3region",
        "us-east-2"
    );
    String awsCredentials = awsAccessKeyId + ":" + awsSecretKey;
    conf.set(
        "parsefilter.archive.credentials",
        awsCredentials
    );

    parseFilter = new ArchiveParseFilter();
    parseFilter.setConf(conf);

    String s3BucketString = conf.getTrimmed("parsefilter.archive.s3bucket");
    String s3RegionString = conf.getTrimmed("parsefilter.archive.s3region");
    String credentials = conf.getTrimmed("parsefilter.archive.credentials");

    assertEquals("Expected bucket to be coherent-commons-digital-assets-dev",
        "coherent-commons-digital-assets-dev", s3BucketString);
    assertEquals("Expected region to be us-east-2",
        "us-east-2", s3RegionString);
    assertTrue(
        "Expected credentials to contain colon",
        credentials.indexOf(":") > 0
    );
    assertTrue(
        "Expected credentials to contain JPxy",
        credentials.indexOf("JPxy") > 0
    );
  }

  /**
   * @throws Exception
   */
  public void testArchiveParseFilterSave() throws Exception {
    // Check that the config has the credentials.
    LOG.info("Testing ArchiveParseFilter");
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", "protocol-file|parse-tika");
    conf.set(
        "parsefilter.archive.s3bucket",
        "coherent-commons-digital-assets-dev"
    );
    conf.set(
        "parsefilter.archive.s3region",
        "us-east-2"
    );
    String awsCredentials = awsAccessKeyId + ":" + awsSecretKey;
    conf.set(
        "parsefilter.archive.credentials",
        awsCredentials
    );
    conf.set("file.content.limit", "-1");

    parseFilter = new ArchiveParseFilter();
    parseFilter.setConf(conf);

    String urlString = "file:" + sampleDir + fileSeparator + "archive-test.pdf";
    Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
    Content content = protocol
        .getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();
    Parse parse = new ParseUtil(conf).parseByExtensionId("parse-tika", content)
        .get(content.getUrl());
    ParseResult parseResult = ParseResult.createParseResult(urlString, parse);
    HTMLMetaTags metaTags = new HTMLMetaTags();
    DocumentFragment doc = null;

    ParseResult result = parseFilter.filter(
        content,
        parseResult,
        metaTags,
        doc
    );

    // Note: this unit test leaves a file at
    // s3://coherent-commons-digital-assets-dev/file/coherencebot-archive/file:/286a1a11ca5efbeea23d5c824c2a8d2c.pdf
    // You can delete that file to test re-archiving it.
    // Otherwise the ArchiveParseFilter will not re-archive it b/c it already exists.
    assertNotNull("Parse result should not be null", result);
  }
}
