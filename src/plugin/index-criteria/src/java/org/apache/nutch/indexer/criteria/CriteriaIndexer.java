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
      // @TODO figure out how to set the status of this document in the segment to 'skipped'
      return null;
    }
    return doc;
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
}
