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
package org.apache.nutch.parse.pdfheadings;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NodeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * A collection of methods for extracting content from DOM trees.
 * 
 * This class holds a few utility methods for pulling content out of DOM nodes.
 * 
 */
public class DOMContentUtils {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Set<String> blockNodes;

  public DOMContentUtils(Configuration conf) {
    setConf(conf);
  }

  public void setConf(Configuration conf) {
    blockNodes = new HashSet<>(
        conf.getTrimmedStringCollection("parser.html.line.separators"));
  }

  /**
   * This method takes a {@link StringBuffer} and a DOM {@link Node}, and will
   * append all the content text found beneath the DOM node to the
   * <code>StringBuffer</code>.
   * 
   * <p>
   * 
   * If <code>abortOnNestedAnchors</code> is true, DOM traversal will be aborted
   * and the <code>StringBuffer</code> will not contain any text encountered
   * after a nested anchor is found.
   * 
   * <p>
   * 
   * @return true if nested anchors were found
   */
  private boolean getText(StringBuffer sb, Node node,
      boolean abortOnNestedAnchors) {
    if (getTextHelper(sb, node, abortOnNestedAnchors, 0)) {
      return true;
    }
    return false;
  }

  /**
   * This is a convinience method, equivalent to
   * {@link #getText(StringBuffer,Node,boolean) getText(sb, node, false)}.
   * 
   */
  public void getText(StringBuffer sb, Node node) {
    getText(sb, node, false);
  }

  // returns true if abortOnNestedAnchors is true and we find nested
  // anchors
  private boolean getTextHelper(StringBuffer sb, Node node,
      boolean abortOnNestedAnchors, int anchorDepth) {
    boolean abort = false;
    NodeWalker walker = new NodeWalker(node);

    while (walker.hasNext()) {

      Node currentNode = walker.nextNode();
      String nodeName = currentNode.getNodeName();
      short nodeType = currentNode.getNodeType();
      Node previousSibling = currentNode.getPreviousSibling();
      if (previousSibling != null
          && blockNodes.contains(previousSibling.getNodeName().toLowerCase())) {
        appendParagraphSeparator(sb);
      } else if (blockNodes.contains(nodeName.toLowerCase())) {
        appendParagraphSeparator(sb);
      }

      if ("script".equalsIgnoreCase(nodeName)) {
        walker.skipChildren();
      }
      if ("style".equalsIgnoreCase(nodeName)) {
        walker.skipChildren();
      }
      if (abortOnNestedAnchors && "a".equalsIgnoreCase(nodeName)) {
        anchorDepth++;
        if (anchorDepth > 1) {
          abort = true;
          break;
        }
      }
      if (nodeType == Node.COMMENT_NODE) {
        walker.skipChildren();
      }
      if (nodeType == Node.TEXT_NODE) {
        // cleanup and trim the value
        String text = currentNode.getNodeValue();
        text = text.replaceAll("\\s+", " ");
        text = text.trim();
        if (text.length() > 0) {
          appendSpace(sb);
          sb.append(text);
        } else {
          appendParagraphSeparator(sb);
        }
      }
    }

    return abort;
  }

  /**
   * Conditionally append a paragraph/line break to StringBuffer unless last
   * character a already indicates a paragraph break. Also remove trailing space
   * before paragraph break.
   *
   * @param buffer
   *          StringBuffer to append paragraph break
   */
  private void appendParagraphSeparator(StringBuffer buffer) {
    if (buffer.length() == 0) {
      return;
    }
    char lastChar = buffer.charAt(buffer.length() - 1);
    if ('\n' != lastChar) {
      // remove white space before paragraph break
      while (lastChar == ' ') {
        buffer.deleteCharAt(buffer.length() - 1);
        lastChar = buffer.charAt(buffer.length() - 1);
      }
      if ('\n' != lastChar) {
        buffer.append('\n');
      }
    }
  }

  /**
   * Conditionally append a space to StringBuffer unless last character is a
   * space or line/paragraph break.
   *
   * @param buffer
   *          StringBuffer to append space
   */
  private void appendSpace(StringBuffer buffer) {
    if (buffer.length() == 0) {
      return;
    }
    char lastChar = buffer.charAt(buffer.length() - 1);
    if (' ' != lastChar && '\n' != lastChar) {
      buffer.append(' ');
    }
  }

  /**
   * This method takes a {@link StringBuffer} and a DOM {@link Node}, and will
   * append the content text found beneath the first <code>title</code> node to
   * the <code>StringBuffer</code>.
   * 
   * @return true if a title node was found, false otherwise
   */
  public boolean getTitle(StringBuffer sb, Node node) {

    NodeWalker walker = new NodeWalker(node);

    while (walker.hasNext()) {

      Node currentNode = walker.nextNode();
      String nodeName = currentNode.getNodeName();
      short nodeType = currentNode.getNodeType();

      if ("body".equalsIgnoreCase(nodeName)) { // stop after HEAD
        return false;
      }

      if (nodeType == Node.ELEMENT_NODE) {
        if ("title".equalsIgnoreCase(nodeName)) {
          getText(sb, currentNode);
          return true;
        }
      }
    }

    return false;
  }

  /**
   * This method takes a DOM {@link Node}, and will return the top 3 headings by
   * font size (for PDF parsed docs).
   * 
   * @return String
   */
  public String getHeading(Node node) {
    StringBuffer sb = new StringBuffer();

    Set<Float> allFontSizes = new HashSet<Float>();
    Set<Float> maxThreeFontSizes = new HashSet<Float>();
    NodeWalker walker = new NodeWalker(node);
    Pattern fontPattern = Pattern.compile("\\[\\d+ (\\d\\d\\.\\d*)\\]");
    Pattern headingPattern = Pattern.compile("(\\[\\d+ \\d\\d\\.\\d*\\])");
    int nHeadings = 0;

    // Initial pass through the nodes.
    // Here we are looking for H1's or font clues left by the PDF2XHTML parsing.
    while (walker.hasNext()) {

      Node currentNode = walker.nextNode();
      short nodeType = currentNode.getNodeType();

      if (nodeType == Node.ELEMENT_NODE) {
        String nodeName = currentNode.getNodeName();
        StringBuffer nodeBuffer = new StringBuffer();
        getText(nodeBuffer, currentNode);
        String nodeText = nodeBuffer.toString();
        if (nodeText.length() > 0) {
          if (nodeName.equalsIgnoreCase("h1")
              || nodeName.equalsIgnoreCase("h2")) {
            if (sb.toString().length() > 0) {
              sb.append(" - ");
            }
            if (nHeadings < 3) {
              sb.append(nodeText);
            }
            nHeadings++;
          } else {
            Matcher m = fontPattern.matcher(nodeText);
            while (m.find()) {
              String fontSize = m.group(1);
              try {
                Float fFontSize = Float.parseFloat(fontSize);
                allFontSizes.add(fFontSize);
              } catch (Exception swallow) {
              }
            }
          }
        }
      }
    }

    for (int i = 0; i < 3 && allFontSizes.size() > 0; i++) {
      Float fMaxFont = Collections.max(allFontSizes);
      maxThreeFontSizes.add(fMaxFont);
      allFontSizes.remove(fMaxFont);
    }

    // For font-based selection...
    // iterate through the nodes and select the first three nodes that have a
    // large font.
    if (maxThreeFontSizes.size() > 0 && nHeadings < 3) {
      walker = new NodeWalker(node);
      while (walker.hasNext()) {

        Node currentNode = walker.nextNode();
        short nodeType = currentNode.getNodeType();
        String nodeName = currentNode.getNodeName();

        if (nodeType == Node.ELEMENT_NODE && nodeName.equalsIgnoreCase("p")) {
          StringBuffer nodeBuffer = new StringBuffer();
          getText(nodeBuffer, currentNode);
          String nodeText = nodeBuffer.toString();
          if (nodeText.length() > 0) {
            Matcher m = fontPattern.matcher(nodeText);
            if (m.find() && nHeadings < 3) {
              String fontSize = m.group(1);
              try {
                Float fFontSize = Float.parseFloat(fontSize);
                if (maxThreeFontSizes.contains(fFontSize)) {
                  String heading = headingPattern.matcher(nodeText)
                      .replaceAll("");
                  if (heading.length() > 0) {
                    if (sb.toString().length() > 0) {
                      sb.append(" - ");
                    }
                    heading = heading.trim();
                    // Prevent really long text blocks being used in entirety.
                    if (heading.split(" ").length > 30) {
                      List<String> strList = Arrays.asList(heading.split(" "));
                      heading = String.join(" ", strList.subList(0, 29));
                    }
                    sb.append(heading);
                    nHeadings++;
                  }
                }
              } catch (Exception e) {
                LOG.error("Cant obtain heading.", e);
              }
            }
          }
        }
      }
    }

    return sb.toString();
  }

  /**
   * This method takes a DOM {@link Node}, and will return the xmptpg:npages.
   * 
   * @return -1 if npages not known or the page count.
   */
  public int getPageCount(Node node) {

    int nPages = -1;

    NodeWalker walker = new NodeWalker(node);

    while (walker.hasNext()) {

      Node currentNode = walker.nextNode();
      String nodeName = currentNode.getNodeName();
      short nodeType = currentNode.getNodeType();

      if (nodeType == Node.ELEMENT_NODE) {
        if ("body".equalsIgnoreCase(nodeName)) { // stop after HEAD
          if (nPages == -1) {
            StringBuffer nodeBuffer = new StringBuffer();
            getText(nodeBuffer, currentNode);
            String nodeText = nodeBuffer.toString();
            // If we didnt find a page count in meta, but we do have a non-zero
            // body length, we set a nominal page count of 1 (to handle for
            // example html)
            if (nodeText.length() > 0) {
              nPages = 1;
            }
          }
          break;
        }

        if ("meta".equalsIgnoreCase(nodeName)) {
          NamedNodeMap attrs = currentNode.getAttributes();
          String nPagesStr = null;
          for (int i = 0; i < attrs.getLength(); i++) {
            Node attr = attrs.item(i);
            if ("content".equalsIgnoreCase(attr.getNodeName())) {
              nPagesStr = attr.getNodeValue();
            }
          }
          for (int i = 0; i < attrs.getLength(); i++) {
            Node attr = attrs.item(i);
            if ("name".equalsIgnoreCase(attr.getNodeName())
                && "xmpTPg:NPages".equalsIgnoreCase(attr.getNodeValue())) {
              if (nPagesStr != null && nPagesStr.length() > 0) {
                try {
                  nPages = Integer.parseInt(nPagesStr);
                  break;
                } catch (Exception ignored) {
                }
              }
            }
          }
        }
      }
    }

    return nPages;
  }

  /** If Node contains a BASE tag then it's HREF is returned. */
  public String getBase(Node node) {

    NodeWalker walker = new NodeWalker(node);

    while (walker.hasNext()) {

      Node currentNode = walker.nextNode();
      String nodeName = currentNode.getNodeName();
      short nodeType = currentNode.getNodeType();

      // is this node a BASE tag?
      if (nodeType == Node.ELEMENT_NODE) {

        if ("body".equalsIgnoreCase(nodeName)) { // stop after HEAD
          return null;
        }

        if ("base".equalsIgnoreCase(nodeName)) {
          NamedNodeMap attrs = currentNode.getAttributes();
          for (int i = 0; i < attrs.getLength(); i++) {
            Node attr = attrs.item(i);
            if ("href".equalsIgnoreCase(attr.getNodeName())) {
              return attr.getNodeValue();
            }
          }
        }
      }
    }

    // no.
    return null;
  }
}
