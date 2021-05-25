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
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.parse.Parse;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.LoggerFactory;

/**
 * Indexing filter that selects documents for the index
 */

public class CriteriaIndexer implements IndexingFilter {
  public final static org.slf4j.Logger LOG = LoggerFactory
      .getLogger(CriteriaIndexer.class);

  private Configuration conf;
  private static Pattern LINE_SPLIT = Pattern.compile("(^.+$)+",
      Pattern.MULTILINE);
  private static Pattern NAME_VALUE_SPLIT = Pattern.compile("(.*?)=(.*)");
  private static Map<String, List<String>> FIELD_FILTER = new HashMap<String, List<String>>();

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
      LOG.info("Skipping document " + url.toString()
          + " due to insuffient text length");
      return null;
    }
    // Choose the best title from title, heading and anchor.
    String[] bestTitleResponse = bestTitle(doc);
    if (bestTitleResponse[0] == null || bestTitleResponse[0].trim().length() == 0) {
      LOG.warn("No title for " + url.toString() + ", skipping");
      return null;
    }
    doc.removeField("title");
    doc.add("title", bestTitleResponse[0]);
    doc.removeField("titleAlgorithm");
    doc.add("title_algorithm", bestTitleResponse[1]);

    // Choose the best translated_title from title_translated and
    // heading_translated.
    String translatedTitle = bestTranslatedTitle(doc, bestTitleResponse[1]);
    if (translatedTitle != null) {
      doc.removeField("title_english");
      doc.add("title_english", translatedTitle);
    }

    // Choose author names that are multi-part.
    String[] authors = bestAuthors(doc);
    if (authors != null) {
      doc.removeField("author");
      doc.add("author", authors);
    }

    // Goal: Point the artifact at the best HTML page that points at this PDF.
    String artifactUrl = bestInlink(doc);
    if (artifactUrl != null) {
      doc.add("referrer_url", artifactUrl);
    } else {
      // If there are no inlinks, the file URL will be the artifact url.
      NutchField urlField = doc.getField("url");
      if (urlField != null) {
        List<Object> urls = urlField.getValues();
        if (urls.size() > 0) {
          Object urlObj = urls.get(0);
          if (urlObj instanceof String) {
            String referrer = (String) urlObj;
            doc.add("referrer_url", referrer);
          }
        }
      }
    }

    String rejectReason = filterTest(doc);

    if (rejectReason != null) {
      LOG.info("Rejecting document " + rejectReason);
      return null;
    } else {
      return doc;
    }
  }

  /**
   * Synthesize a title out of the various candidates in the Nutch document.
   *
   * Chooses from among anchor, heading and title.
   *
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
      cleanHeading = removeExt(cleanHeading
          .substring(0, Math.min(cleanHeading.length(), maxLength)).trim());
    }

    // Make a cleanTitle
    if (source.getField("title") != null) {
      cleanTitle = (String) source.getFieldValue("title");
      // Remove non-printable chars
      cleanTitle = cleanTitle.replaceAll("\\p{C}", "");
      // Shorten to max length and trim.
      cleanTitle = removeExt(cleanTitle
          .substring(0, Math.min(cleanTitle.length(), maxLength)).trim());
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
      cleanAnchor = removeExt(cleanAnchor
          .substring(0, Math.min(cleanAnchor.length(), maxLength)).trim());
    }

    // If we have a title and a heading, choose one of these.
    if (cleanTitle != null && cleanHeading != null) {
      // Prepare normalized variants for matching and analysis.
      String matchTitle = cleanTitle;
      String matchHeading = cleanHeading;

      matchTitle = matchTitle.replaceAll("\\p{Punct}", " ").trim();
      matchHeading = matchHeading.replace(" [" + headingAlgorithm + "]", "");
      matchHeading = matchHeading.replaceAll("\\p{Punct}", " ").trim();

      long tUpper = matchTitle.chars().filter((s) -> Character.isUpperCase(s))
          .count();
      long tLower = matchTitle.chars().filter((s) -> Character.isLowerCase(s))
          .count();
      long hUpper = matchHeading.chars().filter((s) -> Character.isUpperCase(s))
          .count();
      long hLower = matchHeading.chars().filter((s) -> Character.isLowerCase(s))
          .count();
      int tWords = matchTitle.split("\\s+").length;
      int hWords = matchHeading.split("\\s+").length;
      int wordDiff = tWords - hWords;

      if (matchTitle.equals(matchHeading)) {
        String[] returnValue = { cleanTitle, titleAlgorithm };
        return returnValue;
      }
      // If we have a reasonably long title with spaces and a mix of upper and
      // lower, return that.
      if (tWords > 7 && tUpper > 0 && tLower > 5) {
        String[] returnValue = { cleanTitle, titleAlgorithm };
        return returnValue;
      }
      if (hWords > 7 && hUpper > 0 && hLower > 5) {
        String[] returnValue = { cleanHeading, headingAlgorithm };
        return returnValue;
      }

      // Return the longer
      if (wordDiff > 0) {
        String[] returnValue = { cleanTitle, titleAlgorithm };
        return returnValue;
      } else {
        String[] returnValue = { cleanHeading, headingAlgorithm };
        return returnValue;
      }
    }

    if (cleanHeading != null && cleanHeading.length() > 0) {
      String[] returnValue = { cleanHeading, headingAlgorithm };
      return returnValue;
    }

    if (cleanTitle != null && cleanTitle.length() > 0) {
      String[] returnValue = { cleanTitle, titleAlgorithm };
      return returnValue;
    }

    if (cleanAnchor != null && cleanAnchor.length() > 0) {
      String[] returnValue = { cleanAnchor, anchorAlgorithm };
      return returnValue;
    }

    String[] returnValue = { "no title", "no options" };
    return returnValue;
  }

  /**
   * Choose a translated title based on which non-translated title was chosen
   *
   * @param source
   *          = the NutchDocument
   * @param whichTitle
   *          - the title algorithm applied to title selection
   * @return the translated title or null if no translation
   */
  private String bestTranslatedTitle(NutchDocument source, String whichTitle) {
    if (whichTitle.indexOf("PDF") > -1
        && source.getField("heading_english") != null) {
      return (String) source.getFieldValue("heading_english");
    } else if (whichTitle.indexOf("title") > -1
        && source.getField("title_english") != null) {
      return (String) source.getFieldValue("title_english");
    } else if (source.getField("title_english") != null) {
      return (String) source.getFieldValue("title_english");
    } else if (source.getField("heading_english") != null) {
      return (String) source.getFieldValue("heading_english");
    }
    return null;
  }

  /**
   * Parse out of authors any single-word or empty values. leaving only 2 or
   * more word names.
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
   * Determine the best InLink that exists for this document.
   *
   * We choose an inlink within a publication or report section (if named so in a path)
   * or the longest of the inlinks.  Otherwise we return null.
   *
   * @param source
   * @return null if no inlinks, otherwise the best.
   */
  private String bestInlink(NutchDocument source) {
    String bestInLink = null;
    String longestInLink = null;
    String[] publicationKeywords = {"publ", "report", "article", "brief"};

    NutchField inlinkFields = source.getField("inlinks");
    if ( inlinkFields != null) {
      List<Object> inlinks = inlinkFields.getValues();

      for (Object inLinkObj : inlinks) {
        if (inLinkObj instanceof String) {
          String inLinkStr = (String) inLinkObj;
          if (inLinkStr.trim().length() == 0) {
            continue;
          } else if (inLinkStr.toLowerCase().indexOf(".pdf") > 0) {
            // Do use references from other PDFs as the referrer.
            continue;
          }
          if (containsAny(inLinkStr, publicationKeywords) &&
              (bestInLink == null || inLinkStr.length() > bestInLink.length() )) {
            bestInLink = inLinkStr;
          }
          if (longestInLink == null || inLinkStr.length() > longestInLink.length() ) {
            longestInLink = inLinkStr;
          }
        }
      }
    }

    if (longestInLink != null && bestInLink == null) {
      bestInLink = longestInLink;
    }

    return bestInLink;
  }

  /**
   * Check if a string contains any of the provided keywords.
   *
   * @param input
   * @param keywords
   * @return
   */
  private boolean containsAny(String input, String[] keywords) {
    if (input != null && input.length() > 0 && keywords.length > 0) {
      for (String keyword : keywords) {
        if (input.toLowerCase().indexOf(keyword) > -1) {
          return true;
        }
      }
    }
    return false;
  }
  /**
   * Remove an extension from a string for file names. Used in title cleaning
   * because titles in PDFs are often file names.
   *
   * @param input
   *          the string
   * @return the string without the extension
   */
  private String removeExt(String input) {
    if (input == null || input.trim().length() == 0) {
      return input;
    }
    String returnStr = input.trim();
    String[] parts = input.trim().split("\\.");
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
    case "dvi":
    case "htm":
    case "html":
    case "key":
    case "odp":
    case "ods":
    case "pdf":
    case "pps":
    case "ppt":
    case "pptm":
    case "pptx":
    case "qxd":
    case "rtf":
    case "tex":
    case "txt":
    case "wpd":
    case "xls":
    case "xlsx":
      returnStr = String.join(".", Arrays.copyOf(parts, parts.length - 1));
      break;
    }

    return returnStr;
  }

  /**
   * Test every field/keyphrase combination configured into
   * index.criteria.filters to see if there's a reason to reject this document.
   *
   * All testing is done in lowercase. Punctuation is stripped; Whitespace is
   * normalized;
   *
   * @param doc
   * @return a reject explanation, or null if no reason to reject.
   */
  private String filterTest(NutchDocument doc) {
    String reject = null;

    // For every field in the list of filters
    for (Map.Entry<String, List<String>> entry : FIELD_FILTER.entrySet()) {
      String fieldName = entry.getKey();
      NutchField field = doc.getField(fieldName);
      if (field != null) {
        List<Object> fieldContent = field.getValues();
        if (fieldContent != null && fieldContent.size() > 0) {

          // For every value in the field
          for (int fc = 0; fc < fieldContent.size(); fc++) {
            Object fieldObj = fieldContent.get(fc);
            if (fieldObj instanceof String) {
              String fieldValue = (String) fieldObj;
              fieldValue = fieldValue.replaceAll("\\p{Punct}", " ");
              fieldValue = fieldValue.replaceAll("\\s+", " ");
              fieldValue = fieldValue.toLowerCase();
              List<String> keyPhrases = entry.getValue();
              // For every keyword in the phrases for this field...
              for (int i = 0; i < keyPhrases.size(); i++) {
                String keyPhrase = keyPhrases.get(i);
                if (keyPhrase != null && keyPhrase.length() > 0) {
                  int matchIndex = fieldValue.indexOf(keyPhrase);
                  if (matchIndex >= 0) {
                    reject = "Field " + fieldName + " contains " + keyPhrase;
                    return reject;
                  }
                }
              }
            }
          }
          // End of loop through every keyword and every value for this field.
        }
      }
    }

    return reject;
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    String value = conf.get("index.criteria.filters", null);
    if (value != null) {
      LOG.debug("Parsing index.criteria.filters property");
      this.parseConf(value);
    }
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Parse the property value into a set of key phrases by field.
   *
   * Format is
   *
   * field=phrase to check in one field
   * or
   * field1,field2,..=phrase to check in multiple fields
   *
   * Each phrase gets its own line.
   * The phase is case-insensitive.
   * Punctuation is removed from the string.
   * Whitespace is normalized.
   *
   * The results are store in FIELD_FILTER
   * which is keyed by field and contains
   * a list of key phrases to check in that field.
   *
   * @param propertyValue
   */
  private void parseConf(String propertyValue) {
    if (propertyValue == null || propertyValue.trim().length() == 0) {
      return;
    }

    // Split the property into lines
    Matcher lineMatcher = LINE_SPLIT.matcher(propertyValue);
    while (lineMatcher.find()) {
      String line = lineMatcher.group();
      if (line != null && line.length() > 0) {

        // Split the line into field and value. Delimiter is '='.
        Matcher nameValueMatcher = NAME_VALUE_SPLIT.matcher(line.trim());
        if (nameValueMatcher.find()) {
          String fieldName = nameValueMatcher.group(1).trim();
          String value = nameValueMatcher.group(2);
          if (fieldName != null && value != null) {
            String[] fieldNames = fieldName.split(",");
            if (value.length() > 0) {
              for (int i = 0; i < fieldNames.length; i++) {
                fieldName = fieldNames[i].trim();
                List<String> filters = FIELD_FILTER.get(fieldName);
                if (filters == null) {
                  filters = new ArrayList<String>();
                }
                value = value.replaceAll("\\p{Punct}", " ");
                value = value.replaceAll("\\s+", " ");
                value = value.toLowerCase().trim();
                if (value.length() > 0) {
                  filters.add(value);
                  FIELD_FILTER.put(fieldName, filters);
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Used to sort anchors by length descending
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
