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

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.LoggerFactory;

/**
 * Indexing filter that selects documents for the index
 */

public class CriteriaIndexer implements IndexingFilter {
  public final static org.slf4j.Logger LOG = LoggerFactory
      .getLogger(CriteriaIndexer.class);

  private Configuration conf;

  /**
   * The {@link CriteriaIndexer} filter object which selects documents as per
   * configuration setting. See {@code index.criteria} in nutch-default.xml.
   * 
   * @param doc
   *          The {@link NutchDocument} object
   * @param parse
   *          The relevant {@link Parse} object passing through the filter
   * @param url
   *          URL to be filtered for anchor text
   * @param datum
   *          The {@link CrawlDatum} entry
   * @param inlinks
   *          The {@link Inlinks} containing anchor text
   * @return filtered NutchDocument
   */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    int contentLength = parse.getText().length();
    if (contentLength < 4000) {
      LOG.info("Skipping document " + url.toString() + " due to insuffient text length");
      return null;
    }
    // Choose the best title from title, heading and anchor.
    String[] bestTitleResponse = bestTitle(doc);
    doc.removeField("title");
    doc.add("title", bestTitleResponse[0]);
    doc.removeField("titleAlgorithm");
    doc.add("title_algorithm", bestTitleResponse[1]);

    // Choose the best translated_title from title_translated and heading_translated.
    String translatedTitle = bestTranslatedTitle(doc, bestTitleResponse[1]);
    if (translatedTitle != null) {
      doc.removeField("title_english");
      doc.add("title_english", translatedTitle);
    }

    // Choose author names that are multi-part.
    String[] authors = bestAuthors(doc);
    if (authors != null){
      doc.removeField("author");
      doc.add("author", authors);
    }

    return doc;
  }

  /**
   * Synthesize a title out of the various candidates in the Nutch document.
   *
   * Chooses from among anchor, heading and title.
   * @return two values in a string array, [0] = title, [1] = title_algorithm.
   */
  private String[] bestTitle(NutchDocument source) {

    int maxLength = 250;
    String titleAlgorithm = "title";
    String anchorAlgorithm = "anchor";
    String headingAlgorithm = "from PDF fonts";

    // One of these will be the selected title.
    String cleanAnchor = null;
    String cleanHeading = null;
    String cleanTitle = null;

    // Make a clean heading
    if (source.getField("heading") != null) {
      cleanHeading = (String) source.getFieldValue("heading");
      if (cleanHeading.indexOf(" [from PDF fonts]") > -1) {
        headingAlgorithm = "from PDF fonts";
        cleanHeading = cleanHeading.replace(" [" + headingAlgorithm + "]", "");
      } else if (cleanHeading.indexOf(" [from PDF text]") > -1) {
        headingAlgorithm = "from PDF text";
        cleanHeading = cleanHeading.replace(" [" + headingAlgorithm + "]", "");
      }
      // Remove non-printable chars
      cleanHeading = cleanHeading.replaceAll("\\p{C}", "");
      // Shorten to max length and trim.
      cleanHeading = cleanHeading.substring(0, Math.min(cleanHeading.length(), maxLength)).trim();
    }

    // Make a cleanTitle
    if (source.getField("title") != null) {
      cleanTitle = (String) source.getFieldValue("title");
      // Remove non-printable chars
      cleanTitle = cleanTitle.replaceAll("\\p{C}", "");
      // Shorten to max length and trim.
      cleanTitle = removeExt(cleanTitle.substring(0, Math.min(cleanTitle.length(), maxLength)).trim());
    }

    // Make a cleanAnchor
    if (source.getField("anchor") != null) {
      // Anchors are muliply occurring
      List<Object> anchorValues = source.getField("anchor").getValues();
      // Sort by length desc.
      Collections.sort(anchorValues, new compRev());
      cleanAnchor = (String) anchorValues.get(0);
      // Remove non-printable chars
      cleanAnchor = cleanAnchor.replaceAll("\\p{C}", "");
      // Shorten to max length and trim.
      cleanAnchor = removeExt(cleanAnchor.substring(0, Math.min(cleanAnchor.length(), maxLength)).trim());
    }

    // If we have a title and a heading, choose one of these.
    if (cleanTitle != null && cleanHeading != null) {
      // Prepare normalized variants for matching and analysis.
      String matchTitle = cleanTitle;
      String matchHeading = cleanHeading;

      matchTitle = matchTitle.replaceAll("\\p{Punct}", " ").trim();
      matchHeading = matchHeading.replace(" [" + headingAlgorithm + "]", "");
      matchHeading = matchHeading.replaceAll("\\p{Punct}", " ").trim();

      long tUpper = matchTitle.chars().filter((s)->Character.isUpperCase(s)).count();
      long tLower = matchTitle.chars().filter((s)->Character.isLowerCase(s)).count();
      long hUpper = matchHeading.chars().filter((s)->Character.isUpperCase(s)).count();
      long hLower = matchHeading.chars().filter((s)->Character.isLowerCase(s)).count();
      int tWords = matchTitle.split("\\s+").length;
      int hWords = matchHeading.split("\\s+").length;
      int wordDiff = tWords - hWords;

      if (matchTitle.equals(matchHeading)) {
        String[] returnValue = {cleanTitle, titleAlgorithm};
        return returnValue;
      }
      // If we have a reasonably long title with spaces and a mix of upper and lower, return that.
      if (tWords > 7 && tUpper > 0 && tLower > 5) {
        String[] returnValue = {cleanTitle, titleAlgorithm};
        return returnValue;
      }
      if (hWords > 7 && hUpper > 0 && hLower > 5) {
        String[] returnValue = {cleanHeading, headingAlgorithm};
        return returnValue;
      }

      // Return the longer
      if (wordDiff > 0) {
        String[] returnValue = {cleanTitle, titleAlgorithm};
        return returnValue;
      } else {
        String[] returnValue = {cleanHeading, headingAlgorithm};
        return returnValue;
      }
    }

    if (cleanHeading != null && cleanHeading.length() > 0) {
      String[] returnValue = {cleanHeading, headingAlgorithm};
      return returnValue;
    }

    if (cleanTitle != null && cleanTitle.length() > 0) {
      String[] returnValue = {cleanTitle, titleAlgorithm};
      return returnValue;
    }

    if (cleanAnchor != null && cleanAnchor.length() > 0) {
      String[] returnValue = {cleanAnchor, anchorAlgorithm};
      return returnValue;
    }

    String[] returnValue = {"no title", "no options"};
    return returnValue;
  }

  /**
   * Choose a translated title based on which non-translated title was chosen
   *
   * @param source = the NutchDocument
   * @param whichTitle - the title algorithm applied to title selection
   * @return the translated title or null if no translation
   */
  private String bestTranslatedTitle(NutchDocument source, String whichTitle) {
    if (whichTitle.indexOf("PDF") > -1 && source.getField("heading_english") != null) {
      return (String) source.getFieldValue("heading_english");
    } else if (whichTitle.indexOf("title") > -1 && source.getField("title_english") != null) {
      return (String) source.getFieldValue("title_english");
    } else if (source.getField("title_english") != null) {
      return (String) source.getFieldValue("title_english");
    } else if (source.getField("heading_english") != null) {
      return (String) source.getFieldValue("heading_english");
    }
    return null;
  }

  /**
   * Parse out of authors any single-word or empty values.
   * leaving only 2 or more word names.
   */
  private String[] bestAuthors(NutchDocument source) {

    if (source.getField("author") != null) {
        List<String> authors = new ArrayList<String>();
        List<Object> authorList = source.getField("author").getValues();
        for (Object authorObj : authorList) {
          if (authorObj instanceof String) {
            String authorStr = (String) authorObj;
            authorStr = authorStr.trim();
            if (authorStr.length() > 0 && authorStr.indexOf(" ") > 1) {
              authors.add(authorStr);
            }
          }
        }

        if (authors.size() > 0) {
          String[] authorArray = new String[authors.size()];
          authorArray = authors.toArray(authorArray);
          return authorArray;
        }
    }
    return null;
  }

  /**
   * Remove an extension from a string for file names.
   * Used in title cleaning because titles in PDFs are often file names.
   *
   * @param input the string
   * @return the string without the extension
   */
  private String removeExt(String input) {
    if (input == null || input.trim().length() == 0) {
      return input;
    }
    String returnStr = input.trim();
    String [] parts = input.trim().split("\\.");
    if (parts.length == 1) {
      return returnStr;
    }

    String suffix = parts[parts.length - 1].toLowerCase();

    // Remove any query string after the ext.
    if (suffix.indexOf("?") > 2) {
      suffix = suffix.substring(0, suffix.indexOf("?"));
    }

    // If its not a short string, its not an extension.
    if (suffix.length() > 4) {
      return returnStr;
    }

    switch (suffix) {
      case "doc":
      case "docx":
      case "htm":
      case "html":
      case "key":
      case "odp":
      case "ods":
      case "pdf":
      case "pps":
      case "ppt":
      case "pptx":
      case "rtf":
      case "tex":
      case "txt":
      case "wpd":
      case "xls":
      case "xlsx":
        returnStr = String.join(".", Arrays.copyOf(parts, parts.length-1));
        break;
    }

    return returnStr;
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Used to sort anchors by length desciending
   */
  class compRev implements Comparator<Object> {
    public int compare(Object o1, Object o2) {
      if (o1 instanceof String && o2 instanceof String) {
        String s1 = (String) o1;
        String s2 = (String) o2;
        return Integer.compare(s2.length(), s1.length());
      } else {
        return 0;
      }
    }
  }

}
