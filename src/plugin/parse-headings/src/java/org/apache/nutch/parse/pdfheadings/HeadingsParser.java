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

import java.io.ByteArrayInputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.ContentHandler;

/**
 * This parse filter uses Tika to extract the first three major headings from
 * the raw content.
 *
 * It it mainly concerned with PDF texts where it will identify headings based
 * on font sizes greater than 14.0pts and then it will select lines of text with
 * the largest of these fonts from the first page.
 * 
 * It stores the results in the 'heading' meta element in the
 * ParseData.parseMeta returned.
 * 
 * In many cases you will find the title produced by parse-tika to be empty or
 * nonsense for PDFs because it is derived from PDF's meta information which is
 * often uncurated and many PDF publishing operations don't place a meaningful
 * value in this.
 * 
 * So the heading provides an alternative view into the document of its first
 * three major headings. This may or may not contain the actual title, but it is
 * quite likely (for PDFs anyway) to be more helpful in a snippet than the PDF
 * title meta.
 */
public class HeadingsParser implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;
  private TikaConfig tikaConfig = null;
  private DOMContentUtils utils;
  private boolean parseEmbedded = true;
  private boolean upperCaseElementNames = true;

  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment root) {

    String mimeType = content.getContentType();

    LOG.debug("Starting the HeadingParser on mime type " + mimeType);

    // Get the right parser using the mime type as a clue
    Parser parser;
    if ("application/pdf".equalsIgnoreCase(mimeType)) {
      parser = new PDFParser();
    } else {
      CompositeParser compositeParser = (CompositeParser) tikaConfig
          .getParser();
      parser = compositeParser.getParsers().get(MediaType.parse(mimeType));
    }
    if (parser == null) {
      String message = "Can't retrieve Tika parser for mime-type " + mimeType;
      LOG.error(message);
      return parseResult;
    }

    LOG.debug("Using Tika parser {} for mime-type {}.",
        parser.getClass().getName(), mimeType);

    byte[] raw = content.getContent();
    Metadata tikamd = new Metadata();

    ContentHandler domHandler;
    HTMLDocumentImpl doc = new HTMLDocumentImpl();
    doc.setErrorChecking(false);
    DocumentFragment node = doc.createDocumentFragment();
    DOMBuilder domBuilder = new DOMBuilder(doc, node);
    domBuilder.setUpperCaseElementNames(upperCaseElementNames);
    domBuilder.setDefaultNamespaceURI(XHTMLContentHandler.XHTML);
    domHandler = (ContentHandler) domBuilder;

    ParseContext context = new ParseContext();
    if (parseEmbedded) {
      context.set(Parser.class, new AutoDetectParser(tikaConfig));
    }

    tikamd.set(Metadata.CONTENT_TYPE, mimeType);
    try {
      parser.parse(new ByteArrayInputStream(raw), domHandler, tikamd, context);
    } catch (Exception e) {
      LOG.error("Error parsing " + content.getUrl(), e);
      return parseResult;
    }

    String heading = utils.getHeading(node); // extract top headings
    if (heading != null && heading.length() > 0) {
      LOG.debug("HeadingParser produced heading " + heading);
      // Suffix heading with a rubric so later we can tell where it came from.
      String headingPlusRubric = heading + " [from PDF fonts]";
      parseResult.get(content.getUrl()).getData().getParseMeta().set("heading",
          headingPlusRubric);
    }

    int nPages = utils.getPageCount(node);
    if (nPages > 0) {
      LOG.debug("HeadingParser produced pages " + nPages);
      parseResult.get(content.getUrl()).getData().getParseMeta().set("pages",
          Integer.valueOf(nPages).toString());
    }

    // Override the web-based Last-Modified with PDFParser's Last-Modified
    // This ensures copied PDFs still get a correct publication date.
    Object lastModified = tikamd.get("Last-Modified");
    if (lastModified != null) {
      parseResult.get(content.getUrl()).getData().getParseMeta()
          .set("Last-Modified", lastModified.toString());
    }
    return parseResult;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.tikaConfig = null;

    // do we want a custom Tika configuration file
    // deprecated since Tika 0.7 which is based on
    // a service provider based configuration
    String customConfFile = conf.get("tika.config.file");
    if (customConfFile != null) {
      try {
        // see if a Tika config file can be found in the job file
        URL customTikaConfig = conf.getResource(customConfFile);
        if (customTikaConfig != null) {
          tikaConfig = new TikaConfig(customTikaConfig,
              this.getClass().getClassLoader());
        }
      } catch (Exception e1) {
        String message = "Problem loading custom Tika configuration from "
            + customConfFile;
        LOG.error(message, e1);
      }
    }
    if (tikaConfig == null) {
      try {
        tikaConfig = new TikaConfig(this.getClass().getClassLoader());
      } catch (Exception e2) {
        String message = "Problem loading default Tika configuration";
        LOG.error(message, e2);
      }
    }

    utils = new DOMContentUtils(conf);

    upperCaseElementNames = conf.getBoolean("tika.uppercase.element.names",
        true);
    parseEmbedded = conf.getBoolean("tika.parse.embedded", true);
  }

  public Configuration getConf() {
    return this.conf;
  }

}
