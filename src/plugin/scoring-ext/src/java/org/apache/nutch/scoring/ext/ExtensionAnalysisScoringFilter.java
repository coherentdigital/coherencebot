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
package org.apache.nutch.scoring.ext;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

/**
 * This scoring filter will boost URLs with selected extensions
 * during the generator phase.  Specify the scoring.ext.boost (float)
 * and the scoring.ext.extensions to boost.  Add scoring-ext to
 * your plugin includes.
 *
 * @author pciuffetti
 */
public class ExtensionAnalysisScoringFilter implements ScoringFilter {

  private Configuration conf;
  private float extensionBoost = 10.0f;
  private float initialScore = 1.0f;
  private String[] extensionsToBoost = {};

  public ExtensionAnalysisScoringFilter() {
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    extensionBoost = conf.getFloat("scoring.ext.boost", 10.0f);
    extensionsToBoost = conf.getStrings("scoring.ext.extensions");
  }

  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
    return adjust;
  }

  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    // If there are URL extensions to boost during the generator phase
    // match the URL to the extension and boost by the configured value.
    if (extensionsToBoost != null && extensionsToBoost.length > 0) {
      String urlStr = url.toString().toLowerCase();
      for (String extension : extensionsToBoost) {
        if (urlStr.indexOf("." + extension) > 0) {
          float score = datum.getScore() * initSort;
          return score + extensionBoost;
        }
      }
    }
    return datum.getScore() * initSort;
  }

  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
      CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
      throws ScoringFilterException {
    if (dbDatum == null) {
      return initScore;
    }
    return (dbDatum.getScore());
  }

  public void initialScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    datum.setScore(initialScore);
  }

  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
  }

  public void passScoreAfterParsing(Text url, Content content, Parse parse)
      throws ScoringFilterException {
    parse.getData().getContentMeta()
        .set(Nutch.SCORE_KEY, content.getMetadata().get(Nutch.SCORE_KEY));
  }

  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content)
      throws ScoringFilterException {
    content.getMetadata().set(Nutch.SCORE_KEY, "" + datum.getScore());
  }

  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinked) throws ScoringFilterException {
    if (extensionsToBoost != null && extensionsToBoost.length > 0) {
      if (old == null)
        old = datum;
      String urlStr = url.toString().toLowerCase();
      for (String extension : extensionsToBoost) {
        if (urlStr.indexOf("." + extension) > 0) {
          float adjust = old.getScore();
          datum.setScore(adjust + extensionBoost);
        }
      }
    }
  }
}
