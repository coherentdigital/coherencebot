/*
 * Copyright 2021 Coherent Digital LLC
 * https://coherentdigital.net
 */
package org.apache.nutch.analysis.outlinks;

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
  private Configuration conf;

  /**
   * Check the document's outlinks.
   */
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    if (requireDescendants) {
      String fromUrl = content.getUrl();
      Parse parse = parseResult.get(fromUrl);
      ParseData parseData = parse.getData();
      Outlink outlinks[] = parseData.getOutlinks();
      if (outlinks.length > 0) {
        List<Outlink> filteredOutlinks = new ArrayList<Outlink>();
        LOG.info("Checking " + outlinks.length + " outlinks for descendants");
        for (Outlink outlink : outlinks) {
          String toUrl = outlink.getToUrl();
          if (validateDescendant(fromUrl, toUrl)) {
            filteredOutlinks.add(outlink);
          }
        }
        Outlink newOutlinks[] = new Outlink[filteredOutlinks.size()];
        newOutlinks = filteredOutlinks.toArray(newOutlinks);
        LOG.info("Outlinks has " + newOutlinks.length + " descendant url" + ((outlinks.length == 1) ? "" : "s"));
        parseData.setOutlinks(newOutlinks);
        String text = parse.getText();
        ParseResult filteredParseResult = ParseResult.createParseResult(fromUrl,
            new ParseImpl(text, parseData));  
        return filteredParseResult;
      } else {
        return parseResult;
      }
    } else {
      return parseResult;
    }
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
   * @return true if the outlink is a descendant of the referrer.
   */
  protected boolean validateDescendant(String fromUrl, String toUrl) {
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
        if (!(toDomain.equals(fromDomain) && isPdf)) { // not an allowed descendant link
          return false;  // skip it
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
  }

  public Configuration getConf() {
    return this.conf;
  }
}