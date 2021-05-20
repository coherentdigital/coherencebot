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
package org.apache.nutch.analysis.lang;

import java.lang.invoke.MethodHandles;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.translate.AmazonTranslate;
import com.amazonaws.services.translate.AmazonTranslateClient;
import com.amazonaws.services.translate.model.TranslateTextRequest;
import com.amazonaws.services.translate.model.TranslateTextResult;

public class TranslateParser implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static Map<String, String> FIELD_MAP = new HashMap<String, String>();
  private int contentMaxLength = 5000;
  private String targetLang = null;

  private Configuration conf;
  private AmazonTranslate translateClient = null;

  /**
   * Check the document for fields to translate
   */
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    if (FIELD_MAP.size() == 0 || translateClient == null) {
      return parseResult;
    }

    Parse parse = parseResult.get(content.getUrl());
    Metadata parseMeta = parse.getData().getParseMeta();

    String sourceLang = parseMeta.get(Metadata.LANGUAGE);
    if (sourceLang == null) {
      sourceLang = "auto";
    }
    if (sourceLang.equals(targetLang)) {
      return parseResult;
    }

    ArrayList<String> targetFields = new ArrayList<String>();
    StringBuilder textBuilder = new StringBuilder();
    Iterator<Entry<String, String>> it = FIELD_MAP.entrySet().iterator();

    // Translate handles unstructured text. Since we may have more than one
    // field
    // to translate, we put each on their own line.
    while (it.hasNext()) {
      Map.Entry<String, String> pair = (Entry<String, String>) it.next();
      String sourceField = pair.getKey();
      String targetField = pair.getValue();

      // Check both parseMeta and HTMLMeta for the source field.
      String sourceText = parseMeta.get(sourceField);
      if (sourceText == null) {
        sourceText = metaTags.getGeneralTags().get(sourceField);
      }
      // Check if title is to be translated.
      if (sourceText == null && "title".equals(sourceField)) {
        sourceText = parse.getData().getTitle();
      }
      // Add the source field to the translate input.
      // Each field is on its own line.
      if (sourceText != null && sourceText.trim().length() > 0) {
        textBuilder.append(sourceText.trim().replace("\n", " "));
        textBuilder.append("\n");
        targetFields.add(targetField);
      }
    }

    // We translate all inputs at once to reduce the number of calls.
    String sourceText = textBuilder.toString();
    if (sourceText.length() > contentMaxLength) {
      sourceText = sourceText.substring(0, contentMaxLength - 1);
    }
    if (sourceText.length() > 0) {
      String translatedText = translateText(sourceLang, targetLang, sourceText);
      if (translatedText != null && translatedText.length() > 0) {
        String[] sourceFields = sourceText.split("\n");
        String[] translatedFields = translatedText.split("\n");
        for (int i = 0; i < translatedFields.length; i++) {
          String targetField = (i < targetFields.size()) ? targetFields.get(i)
              : "unknown";
          // If the translation resulted in the same text, don't save the
          // translation.
          if (translatedFields[i].equals(sourceFields[i])) {
            continue;
          }
          parseMeta.set(targetField, translatedFields[i]);
        }
      }
    }

    return parseResult;
  }

  /**
   * Translate Text using AWS Translate.
   */
  private String translateText(String sourceLang, String targetLang,
      String text) {
    try {
      TranslateTextRequest request = new TranslateTextRequest().withText(text)
          .withSourceLanguageCode(sourceLang)
          .withTargetLanguageCode(targetLang);
      TranslateTextResult result = translateClient.translateText(request);
      return result.getTranslatedText();
    } catch (Exception e) {
      LOG.error("Unable to translate using AWS Translate: " + e.getMessage());
    }

    return null;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    contentMaxLength = conf.getInt("parse.translate.max.length", 5000);

    targetLang = conf.get("parse.translate.targetlang", "en");
    String[] fields = conf.getStrings("parse.translate.fields");
    if (fields != null) {
      for (int i = 0; i < fields.length; i++) {
        String field = fields[i];
        if (field.length() > 0) {
          String[] parts = field.split("=");
          if (parts.length == 2) {
            FIELD_MAP.put(parts[0], parts[1]);
          }
        }
      }
    }

    if (FIELD_MAP.size() == 0) {
      LOG.error("No fields for translation. Check lang.translate.fields");
    }

    String awsRegionString = conf.getTrimmed("parse.translate.awsregion",
        "us-east-2");
    // Check that the config has the credentials.
    String credentials = conf.getTrimmed("parse.translate.credentials");
    if (credentials == null) {
      String message = "Set AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY in config element parse.translate.credentials. "
          + " This allows using AWS Translate to translate field content.";
      LOG.error(message);
    } else {
      String[] credParts = credentials.split(":", 2);
      if (credParts.length != 2) {
        String message = "Set AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY in config element parse.translate.credentials. "
            + " This allows using AWS Translate to translate field content.";
        LOG.error(message);
      } else {
        String awsAccessKeyId = credParts[0];
        String awsSecretAccessKey = credParts[1];

        try {
          BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKeyId,
              awsSecretAccessKey);

          translateClient = AmazonTranslateClient.builder()
              .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
              .withRegion(Regions.fromName(awsRegionString)).build();
        } catch (Exception e) {
          LOG.error(
              "Unable to initialize AWS Translate Client. " + e.toString());
        }
      }
    }
  }

  public Configuration getConf() {
    return this.conf;
  }
}
