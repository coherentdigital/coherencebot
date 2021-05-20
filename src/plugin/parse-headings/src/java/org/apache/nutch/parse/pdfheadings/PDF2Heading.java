package org.apache.nutch.parse.pdfheadings;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPageTree;
import org.apache.pdfbox.pdmodel.font.PDFontDescriptor;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;

/**
 * PDF2Heading is an implementation of PdfBox's PdfTextStripper designed to
 * extract and tag large font text from the selected page range and present that
 * to the caller as a String.
 * 
 * It it used as a title selector. The calling parser then can choose this
 * large-font text in the manner it sees fit.
 * 
 * See the writeString class for the details of the font-info tags that are
 * added to the text.
 * 
 * @author pciuffetti
 */
public class PDF2Heading extends PDFTextStripper {
  private static final Log LOG = LogFactory.getLog(PDF2Heading.class);
  private int nHeadings = 0;

  /**
   * Construct a PDF2Heading text stripper.
   *
   * @throws IOException
   */
  public PDF2Heading() throws IOException {
    LOG.debug("Initializing PDF2Heading PDFTextStripper");
  }

  /**
   * writeText is overridden to limit the processing of the selected range of
   * pages. The writeText of the base class processes all pages and does not
   * provide a mechanism to select pages.
   */
  @Override
  public void writeText(PDDocument doc, Writer outputStream)
      throws IOException {
    document = doc;
    output = outputStream;
    setSortByPosition(true);
    setShouldSeparateByBeads(true);
    setSuppressDuplicateOverlappingText(true);
    startDocument(doc);
    int startPage = getStartPage();
    int endPage = getEndPage();
    PDPageTree tree = new PDPageTree();
    for (int i = startPage; i <= endPage; i++) {
      // getPage is zero-based, startPage is one-based.
      tree.add(doc.getPage(i - 1));
    }
    processPages(tree);
    endDocument(doc);
  }

  /**
   * This is the key overridden method which inserts font information into the
   * parsed result. The format is
   *
   * [<heading number> <fontsizeinpts>]<headingtext>
   *
   * where heading number is an int starting at 0 and fontsize is a float. It
   * does not include text with small fonts (14.0 pts or less).
   * 
   * At the point where this string is exposed by the underlying
   * PDFTextStripper, it might contain multiple fonts in the same text block. So
   * results like...
   *
   * [1 36.0]Some big title[2 24.0]Some subtitle
   *
   * ...are possible.
   */
  @Override
  protected void writeString(String text, List<TextPosition> textPositions)
      throws IOException {
    StringBuilder builder = new StringBuilder();
    Float prevFontSize = 0.0f;

    for (TextPosition position : textPositions) {
      Float fontSize = position.getFontSizeInPt();
      PDFontDescriptor descriptor = position.getFont().getFontDescriptor();

      boolean headingFont = (fontSize >= 14.0f);
      Float fontWeight = (descriptor != null) ? descriptor.getFontWeight()
          : 0.0f;
      headingFont = headingFont || (Float.compare(fontSize, 10.0f) > 0
          && Float.compare(fontWeight, 400.0f) > 0);
      boolean saveFont = headingFont && (Float.compare(fontSize, prevFontSize) != 0);
      if (saveFont) {
        builder.append("[").append(nHeadings).append(" ").append(fontSize)
            .append("]");
        prevFontSize = fontSize;
        nHeadings++;
      }
      if (headingFont) {
        builder.append(position.getUnicode());
      }
    }

    output.write(builder.toString());
  }
}