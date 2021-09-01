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
package org.apache.nutch.urlfilter.descendants;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLExemptionFilter;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of {@link org.apache.nutch.net.URLExemptionFilter}
 * requires the outlinks to be descendants of the referrer.
 * <p/>
 * Although it extends the URLExemptionFilter it is really an inclusion filter.
 * That is because URLExemptionFilters give access to the fromUrl and toUrl,
 * while URLFilters (which are exclusion filters) give access to only to toUrl
 * in the filter method.
 *
 * It is currently used only in the FetcherThread and by itself does
 * not remove all non-descendants (later parsing adds them back in).
 * So you also need to use plugin 'parsefilter-outlinks' to remove the
 * non-descendants from the parseData outlinks.
 *
 * So if you set 'db.descendant.links' to 'true' and 'db.ignore.external.links'
 * to 'true' and include this plugin, (named urlfilter-descendants) it will add
 * additional exclusion of not only the external outbound links, but also the
 * internal outbound links that are not descendants.
 *
 * A special case is made for toUrls to PDFs since these only need to be from
 * the same domain, not the same domain and descendant path.
 *
 * @since Jun 1, 2021
 * @version 1
 * @author Peter Ciuffetti
 */
public class DescendantUrlSelector implements URLExemptionFilter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String DB_DESCENDANT_LINKS = "db.descendant.links";
  protected boolean requireDescendants = false;
  private Configuration conf;

  @Override
  public boolean filter(String fromUrl, String toUrl) {
    // Return true if we want to keep this outlink, false to discard.
    if (toUrl == null || toUrl.trim().length() == 0) {
      return false;
    }

    if (requireDescendants) {
      try {
        String fromDomain = URLUtil.getDomainName(fromUrl).toLowerCase();
        String toDomain = URLUtil.getDomainName(toUrl).toLowerCase();
        String fromPath = new URL(fromUrl).getPath().toLowerCase();
        String toPath = new URL(toUrl).getPath().toLowerCase();
        String fromChk = fromDomain + fromPath;
        String toChk = toDomain + toPath;

        // Is the outlink not a descendant?
        if (toChk.indexOf(fromChk) != 0) {
          boolean isPdf = (toUrl.indexOf(".pdf") > 0) ? true : false;
          // Handle the special case of PDFs from the same domain are allowed.
          if (!(toDomain.equals(fromDomain) && isPdf)) { // not an allowed
                                                         // descendant link
            return false;
          }
        }
      } catch (MalformedURLException mue) {
        LOG.error("Bad url" + mue.toString());
        return false;
      }
    }
    return true;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    requireDescendants = conf.getBoolean(DB_DESCENDANT_LINKS, false);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Allows this plugin to be tested with URL 'nutch plugin'.
   * 
   * @param args
   *          the from and to URL to test.
   */
  public static void main(String[] args) {

    if (args.length != 2) {
      System.out.println("Error: Invalid Args");
      System.out.println("Usage: nutch plugin "
          + DescendantUrlSelector.class.getName() + " <fromurl> <tourl>");
      return;
    }
    String fromUrl = args[0];
    String toUrl = args[1];
    DescendantUrlSelector instance = new DescendantUrlSelector();
    Configuration conf = NutchConfiguration.create();
    conf.setBoolean(DB_DESCENDANT_LINKS, true);
    instance.setConf(conf);
    System.out.println("Config option 'db.descendant.links' is set to "
        + instance.requireDescendants);
    System.out.println(instance.filter(fromUrl, toUrl));
  }
}
