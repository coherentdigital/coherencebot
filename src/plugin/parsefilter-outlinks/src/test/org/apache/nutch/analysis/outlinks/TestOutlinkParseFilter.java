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
package org.apache.nutch.analysis.outlinks;

import java.lang.invoke.MethodHandles;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.parse.html.HtmlParser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

public class TestOutlinkParseFilter extends TestCase {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  // We have a config here that rejects an Anchor = reject
  private static final String badAnchors =
      "# this is a comment\n" +
      "\n" +
      "reject\n" +
      "\n";
  private Parser parser;
  private Configuration conf;

  public TestOutlinkParseFilter() {
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", "parse-html");
    conf.setBoolean("db.descendant.links", true);
    conf.setBoolean("db.approve.anchors", true);
    parser = new HtmlParser();
    parser.setConf(conf);
  }

  /**
   * Test the Outlink filter removes URLs with bad anchors.
   *
   * @throws Exception
   */
  public void testOutlinkAnchorFilter() throws Exception {

    OutlinkParseFilter filter = new OutlinkParseFilter(badAnchors);
    filter.setConf(conf);

    String htmlText[] = new String[] {
        "<body><html>",
        "<p>this is the extracted text blablabla</p>",
        // Check uppercase variant is eliminated
        "<a href=\"/test1\">Reject</a>",
        // Check lowercase variant is eliminated
        "<a href=\"/test2\">reject</a>",
        // Check whitespace does not affect rejection
        "<a href=\"/test3\">   Reject  \n</a>",
        // Check leading punctuation does not affect rejection
        "<a href=\"/test4\">?.Reject</a>",
        // Check trailing punctuation does not affect rejection
        "<a href=\"/test5\">Reject!#</a>",
        // Check leading and trailing punctuation does not affect rejection
        "<a href=\"/test6\">@*Reject!#</a>",
        // Check that a previously rejected toUrl with a bad anchor also gets removed
        "<a href=\"/test6\">Same page as previous - Should not be kept</a>",
        
        // Add any number of rejected anchors besides this - the one keeper
        // This also tests that a superstring of the bad anchor does not filter.
        "<a href=\"/keep\">Dont Reject</a>",
        "</body></html>"
    };

    String url = "http://nutch.apache.org/";
    Content content = new Content(url, url, String.join("", htmlText).getBytes("UTF-8"), "text/html", new Metadata(), conf);

    byte[] contentBytes = String.join("", htmlText).getBytes("UTF-8");
    Parse parse = parse(url, contentBytes);
    
    ParseResult result = ParseResult.createParseResult(url, parse);
    ParseData parseData = parse.getData();
    Outlink outlinks[] = parseData.getOutlinks();
    assertNotNull("Outlinks were not parsed from the text", outlinks);
    LOG.info("Parsed {} Outlinks from the text before filtering", outlinks.length);
    for (Outlink outlink : outlinks) {
      LOG.info("Outlink before filtering {}", outlink.getToUrl());
    }
    assertEquals("Expected 8 outlinks to be parsed by the HTMLParser",
        8, outlinks.length);

    result = filter.filter(content, result, null, null);

    parseData = parse.getData();
    outlinks = parseData.getOutlinks();
    
    // We expect one outlink to come back, the 'keeper'
    assertEquals("Exxpected all but one outlink to be removed by the filter",
        1, outlinks.length);
    assertEquals("Remaining outlink was not /keep", url + "keep", outlinks[0].getToUrl());
  }

  /**
   * Test the Outlink filter removes non-descendants unless they are a PDF.
   *
   * @throws Exception
   */
  public void testOutlinkDescendantFilter() throws Exception {

    OutlinkParseFilter filter = new OutlinkParseFilter(badAnchors);
    filter.setConf(conf);

    String htmlText[] = new String[] {
        "<body><html>",
        "<p>this is the extracted text blablabla</p>",
        // Check path to sibling is eliminated
        "<a href=\"/docs/sibling\">Reject</a>",
        // Check path to parent is eliminated
        "<a href=\"/docs\">Reject</a>",

        // Add any number of rejected anchors besides this - the two keepers
        // This path should be kept since its relative to the seed.
        "<a href=\"/docs/publications/child\">Dont Reject Child</a>",
        // This path should be kept because it is a PDF
        "<a href=\"/docs/wp-content/child.pdf\">Dont Reject PDF</a>",
        "</body></html>"
    };

    String url = "http://nutch.apache.org/docs/publications/";
    Content content = new Content(url, url, String.join("", htmlText).getBytes("UTF-8"), "text/html", new Metadata(), conf);

    byte[] contentBytes = String.join("", htmlText).getBytes("UTF-8");
    Parse parse = parse(url, contentBytes);

    ParseResult result = ParseResult.createParseResult(url, parse);
    ParseData parseData = parse.getData();
    Outlink outlinks[] = parseData.getOutlinks();
    assertNotNull("Outlinks were not parsed from the text", outlinks);
    LOG.info("Parsed {} Outlinks from the text before filtering", outlinks.length);
    for (Outlink outlink : outlinks) {
      LOG.info("Outlink before filtering {}", outlink.getToUrl());
    }
    assertEquals("Expected 4 outlinks to be parsed by the HTMLParser",
        4, outlinks.length);

    result = filter.filter(content, result, null, null);

    parseData = parse.getData();
    outlinks = parseData.getOutlinks();

    // We expect two outlinks to come back
    assertEquals("Expected all but two outlinks to be removed by the filter",
        2, outlinks.length);
    for (Outlink outlink : outlinks) {
      assertTrue("Saved Outlink should contain 'child'", (outlink.getToUrl().indexOf("child") > 0));
    }
  }

  protected Parse parse(String url, byte[] contentBytes) {
    return parser.getParse(
        new Content(url, url, contentBytes, "text/html",
            new Metadata(), conf)).get(url);
  }
}
