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
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.Loader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.ibm.icu.text.Normalizer2;

/**
 * This parse filter uses PdfBox to extract the first three major headings from
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

  // An array of language codes that use Right to Left script.
  private static String[] rtlLangs = {"ar", "dv", "fa", "ha", "he", "ks", "ps", "ur", "yi"};
  private static List<String> rtlList = new ArrayList<String>(Arrays.asList(rtlLangs));

  private Configuration conf;

  @Override
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment root) {

    String mimeType = content.getContentType();
    byte[] raw = content.getContent();
    InputStream pdfStream = new ByteArrayInputStream(raw);

    LOG.debug("Starting the HeadingParser on mime type " + mimeType);

    try {
      // Get the pdf heading parser
      if ("application/pdf".equalsIgnoreCase(mimeType)) {
        PDF2Heading parser = new PDF2Heading();
        PDDocument document = Loader.loadPDF(pdfStream);
        StringWriter writer = new StringWriter();
        parser.setStartPage(1);
        parser.setEndPage(1);
        parser.writeText(document, writer);
        String output = writer.toString();

        Parse parse = parseResult.get(content.getUrl());
        Metadata parseMeta = parse.getData().getParseMeta();
        String sourceLang = parseMeta.get(Metadata.LANGUAGE);

        // extract top headings
        String heading = getHeading(output);
        if (heading != null && heading.length() > 0) {
          // If the text is RTL, we have to reverse the string.
          if (sourceLang != null) {
            sourceLang = sourceLang.toLowerCase();
            if (rtlList.contains(sourceLang)) {
              heading = reverse(heading);
              // Some scripts (eg Arabic) have 'Presentation' glyphs.
              // This checks if normalization is required and converts it into a form called
              // 'Compatibility Decomposition, followed by Canonical Composition'.
              // See: https://docs.oracle.com/javase/tutorial/i18n/text/normalizerapi.html
              Normalizer2 normalizer = Normalizer2.getNFKCInstance();
              if (!normalizer.isNormalized(heading)) {
                heading = normalizer.normalize(heading);
              }
            }
          }
          LOG.debug("HeadingParser produced heading " + heading);
          // Suffix heading with a rubric so later we can tell where it came
          // from.
          String headingPlusRubric = heading + " [from PDF fonts]";
          parseResult.get(content.getUrl()).getData().getParseMeta()
              .set("heading", headingPlusRubric);
        }

        // Extract npages to parse meta.
        Metadata generalTags = metaTags.getGeneralTags();
        String pages = generalTags.get("xmptpg:npages");
        if (pages != null) {
          int nPages = Integer.parseInt(pages);
          if (nPages > 0) {
            LOG.debug("HeadingParser produced pages " + nPages);
            parseResult.get(content.getUrl()).getData().getParseMeta()
                .set("pages", Integer.valueOf(nPages).toString());
          }
        }

        // Extract Last-Modified to parse meta.
        // Override the web-based Last-Modified with PDFParser's
        // Created-Date or Last-Modified date.
        // This ensures PDF info (if provided) is used as a source of date.
        String lastModified = metaTags.getGeneralTags().get("created");
        if (lastModified == null) {
          lastModified = metaTags.getGeneralTags().get("modified");
        }
        if (lastModified != null) {
          parseResult.get(content.getUrl()).getData().getParseMeta()
              .set("Last-Modified", lastModified);
        }
      }
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return parseResult;
  }

  /**
   * Assemble the parser text into a heading using the embedded font clues.
   *
   * @return a heading or null if not suitable text for a heading.
   */
  String getHeading(String parsedText) {
    StringBuffer sb = new StringBuffer();

    Set<Float> allFontSizes = new HashSet<Float>();
    Set<Float> maxThreeFontSizes = new HashSet<Float>();
    Pattern fontPattern = Pattern.compile("\\[\\d+ (\\d{2,3}.\\d*)\\]");
    Pattern headingPattern = Pattern
        .compile("\\[\\d+ (\\d{2,3}\\.\\d*)\\]([^\\[]+)");
    int nHeadings = 0;
    int nWords = 0;

    Matcher m = fontPattern.matcher(parsedText);
    while (m.find()) {
      String fontSize = m.group(1);
      try {
        Float fFontSize = Float.parseFloat(fontSize);
        allFontSizes.add(fFontSize);
      } catch (Exception swallow) {
      }
    }

    for (int i = 0; i < 3 && allFontSizes.size() > 0; i++) {
      Float fMaxFont = Collections.max(allFontSizes);
      maxThreeFontSizes.add(fMaxFont);
      allFontSizes.remove(fMaxFont);
    }

    // For font-based selection...
    // iterate through the nodes and select the first three nodes that have a
    // large font.
    Float fLastFontSize = 0.0f;
    m = headingPattern.matcher(parsedText);
    while (m.find() && (nHeadings < 3 || nWords < 10)) {
      String fontSize = m.group(1);
      String heading = m.group(2);
      try {
        Float fFontSize = Float.parseFloat(fontSize);
        if (maxThreeFontSizes.contains(fFontSize)) {
          if (heading.length() > 0) {
            heading = heading.trim();
            if (sb.toString().length() > 0) {
              sb.append(" ");
              // Have we switched font sizes?
              if (Float.compare(fLastFontSize,fFontSize) != 0) {
                // On a font size switch with more than one word, treat as a subtitle.
                if (heading.split(" ").length > 1) {
                  sb.append("- ");
                }
              }
            }
            // Prevent really long text blocks being used in entirety.
            if (heading.split(" ").length > 30) {
              List<String> strList = Arrays.asList(heading.split(" "));
              heading = String.join(" ", strList.subList(0, 29));
            }
            sb.append(heading);
            nHeadings++;
            nWords += heading.split(" ").length;
          }
        }
        fLastFontSize = fFontSize;
      } catch (Exception e) {
      }
    }
    return sb.toString();
  }

  /**
   * Reverse an rtl string.
   *
   * @param text
   * @return the reversed string
   */
  private String reverse(String text) {
    StringBuilder sb = new StringBuilder();
    sb.append(text);
    return sb.reverse().toString();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
