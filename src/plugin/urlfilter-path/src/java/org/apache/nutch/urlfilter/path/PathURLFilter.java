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
package org.apache.nutch.urlfilter.path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.util.PrefixStringMatcher;
import org.apache.nutch.util.TrieStringMatcher;
import org.apache.nutch.net.URLFilter;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.StringReader;

import java.util.List;
import java.util.ArrayList;

/**
 * Filters URLs based on a file of URL paths. The file is named by
 * property "urlfilter.path.file" in ./conf/nutch-default.xml
 * This filter is an exclusion filter.  A match causes removal of the URL.
 *
 * <p>
 * The format of this file is one URL path per line.
 * </p>
 */
public class PathURLFilter implements URLFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private TrieStringMatcher trie;

  private Configuration conf;

  public PathURLFilter() throws IOException {

  }

  public PathURLFilter(String stringRules) throws IOException {
    trie = readConfiguration(new StringReader(stringRules));
  }

  @Override
  public String filter(String url) {
    try {
      if (url != null) {
        if (url.indexOf('\\') > 0) {
          url = url.replace('\\', '/');
        }
        URI uri = new URI(url);
        String uriPath = uri.getPath();
        String[] paths = uriPath.split("/");
        for (String path : paths ) {
          // We exclude any URL with a path matching a node from the trie.
          if (path.length() > 0 ) {
            String longestMatch = trie.longestMatch(path);
            boolean sameLength = false;
            if (longestMatch != null) {
              sameLength = (longestMatch.length() == path.length()) ? true : false;
            }
            if (longestMatch != null && sameLength ) {
              LOG.info("Excluding {} due to match of path {} to {}", url, path, longestMatch);
              return null;
            }
          }
        }
      }
    } catch (IndexOutOfBoundsException ioobe) {
      LOG.warn("Index out of bounds processing URL {}", url);
    } catch (URISyntaxException e) {
      LOG.warn("Unable to parse {} into a URI, error {}", url, e.toString());
    }
    return url;
  }

  private TrieStringMatcher readConfiguration(Reader reader) throws IOException {

    BufferedReader in = new BufferedReader(reader);
    List<String> urlpaths = new ArrayList<>();
    String line;

    while ((line = in.readLine()) != null) {
      if (line.length() == 0)
        continue;

      char first = line.charAt(0);
      switch (first) {
      case ' ':
      case '\n':
      case '#': // skip blank & comment lines
        continue;
      default:
        urlpaths.add(line);
      }
    }

    return new PrefixStringMatcher(urlpaths);
  }

  public static void main(String args[]) throws IOException {

    PathURLFilter filter;
    if (args.length >= 1)
      filter = new PathURLFilter(args[0]);
    else
      filter = new PathURLFilter();

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = in.readLine()) != null) {
      String out = filter.filter(line);
      if (out != null) {
        System.out.println(out);
      }
    }
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    String pluginName = "urlfilter-path";

    // precedence hierarchy for definition of filter rules
    // (first non-empty definition takes precedence):
    // 1. string rules defined by `urlfilter.domaindenylist.rules`
    // 2. rule file name defined by `urlfilter.domaindenylist.file`
    // 3. rule file name defined in plugin.xml (`attributeFile`)
    String file = conf.get("urlfilter.path.file");
    Reader reader = null;
    if (file != null) {
      LOG.info("Reading {} rules file {}", pluginName, file);
      reader = conf.getConfResourceAsReader(file);
    }

    if (reader == null) {
      LOG.warn("Missing {} rule file '{}': all URLs will be accepted!",
          pluginName, file);
      trie = new PrefixStringMatcher(new String[0]);
    } else {
      try {
        trie = readConfiguration(reader);
      } catch (IOException e) {
        LOG.error("Error reading " + pluginName + " rule file " + file, e);
      }
    }
  }

  public Configuration getConf() {
    return this.conf;
  }
}
