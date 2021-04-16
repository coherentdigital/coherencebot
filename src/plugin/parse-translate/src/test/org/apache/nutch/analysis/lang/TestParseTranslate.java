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

// Nutch imports
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.w3c.dom.DocumentFragment;
import org.apache.html.dom.HTMLDocumentImpl;
import org.junit.Assert;
import org.junit.Test;

public class TestParseTranslate {

  private static String URL = "http://foo.bar/";

  private static String BASE = "http://foo.bar/";

  String docs[] = {
      "<html lang=\"fi\"><head><title>Istuntokauden uudelleenavaaminen</title></head><body>Julistan perjantaina joulukuun 17. päivänä keskeytetyn Euroopan parlamentin istunnon avatuksi ja esitän vielä kerran vilpittömän toiveeni siitä, että teillä olisi ollut oikein mukava joululoma.</body></html>",
      "<html><head><meta http-equiv=\"content-language\" content=\"fr\"><title>Reprise de la session</title></head><body>Je déclare reprise la session du Parlement européen qui avait été interrompue le vendredi 17 décembre dernier et je vous renouvelle tous mes vux en espérant que vous avez passé de bonnes vacances.</body></html>",
      "<html><head><meta name=\"dc.language\" content=\"nl\"><title>Hervatting van de sessie</title></head><body>Ik verklaar de zitting van het Europees Parlement, die op vrijdag 17 december werd onderbroken, te zijn hervat. Ik wens u allen een gelukkig nieuwjaar en hoop dat u een goede vakantie heeft gehad.</body></html>" };

  String metalanguages[] = {"fi", "fr", "nl"};

  String expectedTranslations[] = {"Reopening of the session", "Resumed session", "Resumption of session"};

  /**
   * Test parsing of language identifiers from html
   */
  @Test
  public void testLanguageParsing() {
    try {
      /* loop through the test documents and validate the language detection result */
      Configuration conf = NutchConfiguration.create();
      conf.set("plugin.includes","parse-(html|headings|metatags)|language-identifier");
      conf.set("lang.extraction.policy","detect");
      ParseUtil parser = new ParseUtil(conf);
      for (int t = 0; t < docs.length; t++) {
        Content content = getContent(docs[t], conf);
        Parse parse = parser.parse(content).get(content.getUrl());
        Assert.assertEquals(metalanguages[t], (String) parse.getData()
            .getParseMeta().get(Metadata.LANGUAGE));
      }
    } catch (Exception e) {
      e.printStackTrace(System.out);
      Assert.fail(e.toString());
    }
  }

  private Content getContent(String text, Configuration conf) {
    Metadata meta = new Metadata();
    meta.add("Content-Type", "text/html");
    return new Content(URL, BASE, text.getBytes(), "text/html", meta, conf);
  }

  /**
   * Test title translation
   */
  @Test
  public void testAwsTranslate() {
    Configuration conf = NutchConfiguration.create();
    conf.set("lang.extraction.policy", "detect");
    conf.set("plugin.includes", "parse-(html|headings|metatags|translate)|language-identifier");
    conf.set("parse.translate.fields", "title=title_english");
    conf.set("parse.translate.targetlang", "en");
    conf.set("parse.translate.credentials","AKIA23B6R4NTDV2KFE65:s6E9FZAVypzEPhdhTtuDR9fektfASVl59UbwqxOF");
    conf.set("parse.translate.awsregion", "us-east-2");
    conf.setInt("parse.translate.max.length", 2048);

    try {
      ParseUtil parser = new ParseUtil(conf);
      /* loop through the test documents and validate the translation result */
      for (int t = 0; t < docs.length; t++) {
        Content content = getContent(docs[t], conf);
        Parse parse = parser.parse(content).get(content.getUrl());
        Assert.assertNotNull(
          "The parseData must exist in the parse",
          parse.getData());
        Assert.assertNotNull(
          "The parseMata must exist in the parseData",
          parse.getData().getParseMeta());
        String titleEn = (String) parse.getData().getParseMeta().get("title_english");
        Assert.assertNotNull(
          "The parseMata must contain a title_english", titleEn);
        Assert.assertTrue(
          "The title_english for document " + t + " must be " + expectedTranslations[t] + " but was " + titleEn,
          titleEn.equals(expectedTranslations[t]));
      }
    } catch (Exception e) {
      Assert.fail("Exception" + e.toString());
    }
  }
}
