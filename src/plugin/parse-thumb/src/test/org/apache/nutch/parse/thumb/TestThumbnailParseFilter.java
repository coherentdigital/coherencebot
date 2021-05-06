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
package org.apache.nutch.parse.thumb;

import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

import java.io.IOException;

import org.w3c.dom.DocumentFragment;
import org.xml.sax.SAXException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestThumbnailParseFilter {
  private static Configuration conf = NutchConfiguration.create();
  private StringBuffer sb = new StringBuffer();

  @Before
  public void setUp() {
    String contentGenerator = "All work and no play makes Jack a dull boy. ";
    for (int i = 0; i < 1000; i++) {
      sb.append(contentGenerator);
    }
  }

  @Test
  public void testHtmlThumbCreate() throws IOException, SAXException {

    String url = "https://coherentdigital.net";
    conf.setStrings("parsefilter.thumb.s3bucket", "coherent-images");
    conf.setStrings("parsefilter.thumb.serviceurl",
        "https://image.thum.io/get/auth/{auth}/pdfSource/fullpage/width/400/png/noanimate/?url=");
    // @TODO: Get the AWS credentials from the env
    conf.setStrings("parsefilter.thumb.credentials",
        "AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY:9343-a2bf76236ec823c6ceaaf37cc591f4c5");
    conf.setStrings("parsefilter.thumb.s3region", "us-east-2");
    HtmlParseFilter filter = new org.apache.nutch.parse.thumb.ThumbnailParseFilter();
    filter.setConf(conf);

    Content content = new Content(url, url, sb.toString().getBytes("UTF8"),
        "text/html; charset=UTF-8", new Metadata(), conf);
    ParseResult parseResult = new ParseStatus().getEmptyParseResult(url, conf);

    HTMLMetaTags metaTags = new HTMLMetaTags();
    DocumentFragment node = new HTMLDocumentImpl().createDocumentFragment();

    parseResult = filter.filter(content, parseResult, metaTags, node);

    Assert.assertNotNull("The url must exist in the content", content.getUrl());
    Assert.assertNotNull("The parse must exist in the parseResult",
        parseResult.get(content.getUrl()));
    Assert.assertNotNull("The parseData must exist in the parse",
        parseResult.get(content.getUrl()).getData());
    Assert.assertNotNull("The parseMata must exist in the parseData",
        parseResult.get(content.getUrl()).getData().getParseMeta());
    String thumbnail = parseResult.get(content.getUrl()).getData()
        .getParseMeta().get("thumbnail");
    Assert.assertNotNull("The parseMata must contain a thumbnail", thumbnail);
    Assert.assertTrue("The thumbnail must have an image URL in the metadata",
        thumbnail.indexOf(".png") > 0);
  }

  @Test
  public void testPdfThumbCreate() throws IOException, SAXException {

    // The MICO VPAT
    String url = "https://d1qq4p6p3fiwyq.cloudfront.net/product-information/Mindscape_VPAT_2.4_13-Jan-2021.pdf";
    conf.setStrings("parsefilter.thumb.s3bucket", "coherent-images");
    conf.setStrings("parsefilter.thumb.serviceurl",
        "https://image.thum.io/get/auth/{auth}/pdfSource/fullpage/width/400/png/noanimate/?url=");
    // @TODO: Get the AWS credentials from the env
    conf.setStrings("parsefilter.thumb.credentials",
        "AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY:9343-a2bf76236ec823c6ceaaf37cc591f4c5");
    conf.setStrings("parsefilter.thumb.s3region", "us-east-2");
    HtmlParseFilter filter = new org.apache.nutch.parse.thumb.ThumbnailParseFilter();
    filter.setConf(conf);

    Content content = new Content(url, url, sb.toString().getBytes("UTF8"),
        "text/html; charset=UTF-8", new Metadata(), conf);

    ParseResult parseResult = new ParseStatus().getEmptyParseResult(url, conf);

    HTMLMetaTags metaTags = new HTMLMetaTags();
    DocumentFragment node = new HTMLDocumentImpl().createDocumentFragment();

    parseResult = filter.filter(content, parseResult, metaTags, node);

    Assert.assertNotNull("The url must exist in the content", content.getUrl());
    Assert.assertNotNull("The parse must exist in the parseResult",
        parseResult.get(content.getUrl()));
    Assert.assertNotNull("The parseData must exist in the parse",
        parseResult.get(content.getUrl()).getData());
    Assert.assertNotNull("The parseMata must exist in the parseData",
        parseResult.get(content.getUrl()).getData().getParseMeta());
    String thumbnail = parseResult.get(content.getUrl()).getData()
        .getParseMeta().get("thumbnail");
    Assert.assertNotNull("The parseMata must contain a thumbnail", thumbnail);
    Assert.assertTrue("The thumbnail must have an image URL in the metadata",
        thumbnail.indexOf(".png") > 0);
  }

  @Ignore("Till later ")
  public void testThumbAlreadyExists() throws IOException, SAXException {

    String url = "https:coherentdigital.net";
    conf.setStrings("parsefilter.thumb.s3bucket", "coherent-images");
    conf.setStrings("parsefilter.thumb.serviceurl",
        "https://image.thum.io/get/auth/{auth}/{url}");
    // @TODO: Get the AWS credentials from the env
    conf.setStrings("parsefilter.thumb.credentials",
        "AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY:9343-a2bf76236ec823c6ceaaf37cc591f4c5");
    conf.setStrings("parsefilter.thumb.s3region", "us-east-2");

    HtmlParseFilter filter = new org.apache.nutch.parse.thumb.ThumbnailParseFilter();
    filter.setConf(conf);

    Content content = new Content(url, url, "".getBytes("UTF8"),
        "text/html; charset=UTF-8", new Metadata(), conf);

    ParseResult parseResult = new ParseStatus().getEmptyParseResult(url, conf);

    HTMLMetaTags metaTags = new HTMLMetaTags();
    DocumentFragment node = new HTMLDocumentImpl().createDocumentFragment();

    parseResult = filter.filter(content, parseResult, metaTags, node);

    Assert.assertNotNull("The url must exist in the content", content.getUrl());
    Assert.assertNotNull("The parse must exist in the parseResult",
        parseResult.get(content.getUrl()));
    Assert.assertNotNull("The parseData must exist in the parse",
        parseResult.get(content.getUrl()).getData());
    Assert.assertNotNull("The parseMata must exist in the parseData",
        parseResult.get(content.getUrl()).getData().getParseMeta());
    Assert.assertNotNull("The thumbnail must exist in the parseMeta",
        parseResult.get(content.getUrl()).getData().getParseMeta()
            .get("thumbnail"));
    Assert.assertEquals(
        "The thumbnail must have the correct value in the metadata",
        "coherentdigital-net/coherentdigital-net.png", parseResult
            .get(content.getUrl()).getData().getParseMeta().get("thumbnail"));
  }
}
