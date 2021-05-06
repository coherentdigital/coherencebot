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
package org.apache.nutch.parse.pdfheadings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Before;
import org.w3c.dom.DocumentFragment;

/**
 * Base class to extend Heading parser tests from.
 */
public class HeadingParserTest {

  protected String fileSeparator = System.getProperty("file.separator");

  /**
   * Folder with test data, defined in src/plugin/build-plugin.xml. Make sure
   * that all sample files are copied to "test.data", they must be listed in
   * src/plugin/parse-heading/build.xml
   */
  protected String sampleDir = System.getProperty("test.data", ".");

  protected Configuration conf;

  @Before
  public void setUp() {
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", "protocol-file|parse-tika|parse-headings");
    conf.set("file.content.limit", "-1");
    conf.set("parser.timeout", "-1");
  }

  public Metadata getHeadingParseMeta(String fileName)
      throws ProtocolException, ParseException {
    String urlString = "file:" + sampleDir + fileSeparator + fileName;
    Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
    Content content = protocol
        .getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();
    HTMLMetaTags metaTags = new HTMLMetaTags();

    HtmlParseFilter filter = new HeadingsParser();
    filter.setConf(conf);
    DocumentFragment node = new HTMLDocumentImpl().createDocumentFragment();

    ParseResult parseResult = new ParseStatus().getEmptyParseResult(urlString,
        conf);

    parseResult = filter.filter(content, parseResult, metaTags, node);

    return parseResult.get(urlString).getData().getParseMeta();
  }
}
