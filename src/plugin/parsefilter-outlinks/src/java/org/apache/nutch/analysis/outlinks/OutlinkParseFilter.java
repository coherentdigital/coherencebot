/*
 * Copyright 2021 Coherent Digital LLC
 * https://coherentdigital.net
 */
package org.apache.nutch.analysis.outlinks;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.PrefixStringMatcher;
import org.apache.nutch.util.TrieStringMatcher;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * The OutlinkParseFilter will remove outlinks that don't satisfy a condition.
 *
 * In this implementation, the OutlinkParseFilter will remove outlinks that
 * are not descendants of the fromUrl.  To activate this filter, add
 * 'parserfilter-outlinks' to the plugin.includes.
 * And add 'db.descendant.links' = true to the nutch-site.xml.
 *
 * This helps control the crawl to stay within a certain section of a seed URL.
 *
 * @author Peter Ciuffetti
 */
public class OutlinkParseFilter implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private boolean requireDescendants = false;
  private boolean approveAnchors = false;
  private Configuration conf;
  private TrieStringMatcher trie = null;

  public OutlinkParseFilter() throws IOException {
  }

  public OutlinkParseFilter(String stringRules) throws IOException {
    trie = readConfiguration(new StringReader(stringRules));
  }

  /**
   * Check the document's outlinks.
   */
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    ParseData parseData = null;
    Parse parse = null;
    String fromUrl = content.getUrl();
    ParseResult filteredParseResult = parseResult;

    // First we remove any outlinks that are not descendants of the seed.
    if (requireDescendants) {
      parse = parseResult.get(fromUrl);
      parseData = parse.getData();
      String seedUrl = parseData.getContentMeta().get("collection.seed");
      if (seedUrl == null) {
        seedUrl = fromUrl;
      }
      String allowedDomainsStr = parseData.getContentMeta().get("org.domains");
      String allowedDomains[] = new String[0];
      if (allowedDomainsStr != null) {
        allowedDomains = allowedDomainsStr.split(";");
      }

      Outlink outlinks[] = parseData.getOutlinks();
      if (outlinks.length > 0) {
        List<Outlink> filteredOutlinks = new ArrayList<Outlink>();
        LOG.info("Checking " + outlinks.length + " outlinks for descendants of " + seedUrl);
        for (Outlink outlink : outlinks) {
          String toUrl = outlink.getToUrl();
          if (validateDescendant(seedUrl, toUrl, allowedDomains)) {
            filteredOutlinks.add(outlink);
          } else {
            LOG.info("Rejecting outlink {}", outlink);
          }
        }
        Outlink newOutlinks[] = new Outlink[filteredOutlinks.size()];
        newOutlinks = filteredOutlinks.toArray(newOutlinks);
        LOG.info("Outlinks has " + newOutlinks.length + " descendant url" + ((outlinks.length == 1) ? "" : "s"));
        parseData.setOutlinks(newOutlinks);
        String text = parse.getText();
        filteredParseResult = ParseResult.createParseResult(fromUrl,
            new ParseImpl(text, parseData));  
      }
    }

    // Next we remove outlinks that have an anchor exclusion (if applicable)
    if (approveAnchors) {
      parse = filteredParseResult.get(fromUrl);
      parseData = parse.getData();
      Outlink outlinks[] = parseData.getOutlinks();
      if (outlinks.length > 0) {
        // Cycle through all outlinks and build a list of excluded toURLs
        // based on anchor matching.  A toURL could be on this list multiple
        // times with different anchors.  Any anchor rejected will exclude all
        // toURLs with that anchor.
        List<String> excludedUrls = new ArrayList<String>();
        for (Outlink outlink : outlinks) {
          String originalAnchor = outlink.getAnchor();
          if (originalAnchor != null && originalAnchor.length() > 0) {
            String anchor = originalAnchor.trim().toLowerCase();
            // This strips leading and trailing punctuation
            anchor = anchor.replaceAll("^\\p{P}*(.*?)\\p{P}*$", "$1");
            if (anchor.length() > 0) {
              String longestMatch = trie.longestMatch(anchor);
              if (longestMatch != null) {
                // An exact match is if they are the same length.
                if (longestMatch.length() == anchor.length()) {
                  excludedUrls.add(outlink.getToUrl());
                }
              }
            }
          }
        }
        if (excludedUrls.size() > 0) {
          LOG.info("Excluding {} URLs based on anchor filtering",
              excludedUrls.size());
          // Update the outlinks removing any with excluded URLs.
          List<Outlink> filteredOutlinks = new ArrayList<Outlink>();
          for (Outlink outlink : outlinks) {
            String toUrl = outlink.getToUrl();
            if (!excludedUrls.contains(toUrl)) {
              // URL is not excluded, do not filter
              filteredOutlinks.add(outlink);
            } else {
              LOG.info("Rejecting url by anchor {}", toUrl);
            }
          }
          Outlink newOutlinks[] = new Outlink[filteredOutlinks.size()];
          newOutlinks = filteredOutlinks.toArray(newOutlinks);
          LOG.info("Outlinks has " + newOutlinks.length + " approved anchor" +
              ((newOutlinks.length == 1) ? "" : "s"));
          parseData.setOutlinks(newOutlinks);
          String text = parse.getText();
          filteredParseResult = ParseResult.createParseResult(fromUrl,
              new ParseImpl(text, parseData));
        } else {
          LOG.info("All Outlinks have approved anchors");
        }
      }
    }
    return filteredParseResult;
  }

  /**
   * Checks if a toUrl is a valid descendant of the fromUrl.
   *
   * In this context a descendant will if the host+path of the
   * fromUrl is a prefix of the host+path of the toUrl.
   *
   * A special case is made for PDFs.  In this case we only
   * require that the PDF is from the same domain of the referrer.
   *
   * @param fromUrl the referring URL
   * @param toUrl the outlink
   * @param allowedDomains - valid domains for PDFs
   * @return true if the outlink is a descendant of the referrer.
   */
  protected boolean validateDescendant(String fromUrl, String toUrl, String allowedDomains[]) {
    try {
      if (fromUrl.indexOf('\\') > 0) {
        fromUrl = fromUrl.replace('\\', '/');
      }
      if (toUrl.indexOf('\\') > 0) {
        toUrl = toUrl.replace('\\', '/');
      }
      String fromDomain = URLUtil.getDomainName(fromUrl).toLowerCase();
      String toDomain = URLUtil.getDomainName(toUrl).toLowerCase();
      String fromHost = URLUtil.getHost(fromUrl).toLowerCase();
      String toHost = URLUtil.getHost(toUrl).toLowerCase();
      String fromPath = new URL(fromUrl).getPath().toLowerCase();
      String toPath = new URL(toUrl).getPath().toLowerCase();
      String fromChk = fromHost + fromPath;
      String toChk = toHost + toPath;

      // Is the outlink not a descendant?
      if (toChk.indexOf(fromChk) != 0) {
        boolean isPdf = (toUrl.indexOf(".pdf") > 0) ? true : false;
        // Handle the special case of PDFs from allowed domains.
        if (isPdf) {
          // See if the PDF domain is an allowed domain.
          for (String pdfDomain : allowedDomains) {
            if (pdfDomain.equals(toDomain)) {
              LOG.info("Accepting allowed PDF domain {}", toUrl);
              return true;
            } else if (pdfDomain.equals(toHost)) {
              // Allow for CDN domains like bucket.s3.amazonaws.com
              LOG.info("Accepting allowed PDF host {}", toUrl);
              return true;
            }
          }
          return false;
        } else {
          return false; // not an allowed descendant link, skip it.
        }
      }
    } catch (MalformedURLException mue) {
      return false;
    }

    return true;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    requireDescendants = conf.getBoolean("db.descendant.links", false);
    approveAnchors = conf.getBoolean("db.approve.anchors", false);

    if (approveAnchors && trie == null) {
      // Read in the anchor texts that will be filtered out.
      String file = conf.get("urlfilter.anchor.file");
      Reader reader = null;
      if (file != null) {
        LOG.info("Reading urlfitler.anchor.file rules file {}", file);
        reader = conf.getConfResourceAsReader(file);
      }

      if (reader == null) {
        LOG.warn("Missing rule file '{}': all Anchors will be accepted!",  file);
        trie = new PrefixStringMatcher(new String[0]);
      } else {
        try {
          trie = readConfiguration(reader);
        } catch (IOException e) {
          LOG.error("Error reading rule file {} {}" + file, e);
        }
      }
    }

  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Read in the anchor texts that will be excluded.
   *
   * @param reader
   * @return
   * @throws IOException
   */
  private TrieStringMatcher readConfiguration(Reader reader) throws IOException {

    BufferedReader in = new BufferedReader(reader);
    List<String> anchorTexts = new ArrayList<>();
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
        anchorTexts.add(line);
      }
    }

    return new PrefixStringMatcher(anchorTexts);
  }
}