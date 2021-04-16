package org.apache.nutch.parse.pdfheadings;

import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.lang.invoke.MethodHandles;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.font.PDFontDescriptor;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;
import org.apache.pdfbox.util.Matrix;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that overrides the {@link PDFTextStripper} functionality
 * to produce a semi-structured XHTML SAX events instead of a plain text
 * stream.
 * 
 * PDC: This is a local implementation of the same class in Tika.
 * I needed to override the writeString method in the original class
 * so I could get my greasy hands on the font information that the PdfTextStripper
 * does not include.  And, sadly, the original class was not public so I couldn't
 * extend and override.  To reduce the code size, I removed some methods not used
 * by the configuration.
 * 
 * Sorry for the cut and paste!...
 * 
 */
class PDF2XHTML extends AbstractPDF2XHTML {

      private static final Logger LOG = LoggerFactory
          .getLogger(MethodHandles.lookup().lookupClass());

    private int nHeadings = 0;
    private int nPages = 0;

    PDF2XHTML(PDDocument pdDocument, ContentHandler handler, ParseContext context, Metadata metadata,
            PDFParserConfig config) throws IOException {
        super(pdDocument, handler, context, metadata, config);
    }

    /**
     * Converts the given PDF document (and related metadata) to a stream
     * of XHTML SAX events sent to the given content handler.
     *
     * @param document PDF document
     * @param handler  SAX content handler
     * @param metadata PDF metadata
     * @throws SAXException  if the content handler fails to process SAX events
     * @throws TikaException if there was an exception outside of per page processing
     */
    public static void process(
            PDDocument document, ContentHandler handler, ParseContext context, Metadata metadata,
            PDFParserConfig config)
            throws SAXException, TikaException {
        PDF2XHTML pdf2XHTML = null;
        try {
            pdf2XHTML = new PDF2XHTML(document, handler, context, metadata, config);
            config.configure(pdf2XHTML);

            pdf2XHTML.writeText(document, new Writer() {
                @Override
                public void write(char[] cbuf, int off, int len) {
                }

                @Override
                public void flush() {
                }

                @Override
                public void close() {
                }
            });
        } catch (IOException e) {
            if (e.getCause() instanceof SAXException) {
                throw (SAXException) e.getCause();
            } else {
                throw new TikaException("Unable to extract PDF content", e);
            }
        }
        if (pdf2XHTML.exceptions.size() > 0) {
            //throw the first
            throw new TikaException("Unable to extract PDF content", pdf2XHTML.exceptions.get(0));
        }
    }


/**
 * This method is overridden to skip any page except page one.
 */
@Override
public void processPage(PDPage page) throws IOException {
    try {
        if (nPages == 0) {
            super.processPage(page);
        }
        nPages++;
    } catch (IOException e) {
        handleCatchableIOE(e);
        endPage(page);
    }
}

/**
 * This method is overridden to skip any page except page one.
 */
@Override
protected void endPage(PDPage page) throws IOException {
    try {
        if (nPages == 1) {
          writeParagraphEnd();
          super.endPage(page);
        }
    } catch (IOException e) {
        handleCatchableIOE(e);
    }
}


@Override
protected void writeParagraphStart() throws IOException {
    super.writeParagraphStart();
    try {
        xhtml.startElement("p");
    } catch (SAXException e) {
        throw new IOException("Unable to start a paragraph", e);
    }
}

@Override
protected void writeParagraphEnd() throws IOException {
    super.writeParagraphEnd();
    try {
        xhtml.endElement("p");
    } catch (SAXException e) {
        throw new IOException("Unable to end a paragraph", e);
    }
}

@Override
protected void writeString(String text) throws IOException {
    try {
        xhtml.characters(text);
    } catch (SAXException e) {
        throw new IOException(
                "Unable to write a string: " + text, e);
    }
}

/**
 * This is the key overridden method which inserts font information
 * into the parsed result.  The format is 
 *
 * [<heading number> <fontsizeinpts>]<headingtext>
 *
 * where heading number is an int starting at 0 and fontsize is a float.
 * It does not include text with small fonts (14.0 pts or less).
 * 
 * At the point where this string is exposed by the underlying PDFTextStripper, 
 * it might contain multiple fonts in the same text block.  So results like...
 *
 * [1 36.0]Some big title[2 24.0]Some subtitle
 *
 * ...are possible.
 */
@Override
protected void writeString(String text, List<TextPosition> textPositions) throws IOException {
    StringBuilder builder = new StringBuilder();
    Float prevFontSize = 0.0f;

    for (TextPosition position : textPositions) {
        Float fontSize = position.getFontSizeInPt();
        PDFontDescriptor descriptor = position.getFont().getFontDescriptor();

        boolean headingFont = (fontSize >= 14.0f);
        Float fontWeight = (descriptor != null) ? descriptor.getFontWeight() : 0.0f;
        headingFont = headingFont || (fontSize > 10.0f && fontWeight > 400.0f); 
        boolean saveFont = headingFont && !fontSize.equals(prevFontSize);
        if (saveFont) {
          builder.append("[").append(nHeadings).append(" ").append(fontSize).append("]");
          prevFontSize = fontSize;
          nHeadings++;
        }
        if (headingFont) {
          builder.append(position.getUnicode());
        }
    }

    writeString(builder.toString());
}

@Override
protected void writeCharacters(TextPosition text) throws IOException {
    try {
        xhtml.characters(text.getUnicode());
    } catch (SAXException e) {
        throw new IOException(
                "Unable to write a character: " + text.getUnicode(), e);
    }
}

@Override
protected void writeWordSeparator() throws IOException {
    try {
        xhtml.characters(getWordSeparator());
    } catch (SAXException e) {
        throw new IOException(
                "Unable to write a space character", e);
    }
}

@Override
protected void writeLineSeparator() throws IOException {
    try {
        xhtml.newline();
    } catch (SAXException e) {
        throw new IOException(
                "Unable to write a newline character", e);
    }
}

class AngleCollector extends PDFTextStripper {
    Set<Integer> angles = new HashSet<>();

    public Set<Integer> getAngles() {
        return angles;
    }

    /**
     * Instantiate a new PDFTextStripper object.
     *
     * @throws IOException If there is an error loading the properties.
     */
    AngleCollector() throws IOException {
    }

    @Override
    protected void processTextPosition(TextPosition text) {
        Matrix m = text.getTextMatrix();
        m.concatenate(text.getFont().getFontMatrix());
        int angle = (int) Math.round(Math.toDegrees(Math.atan2(m.getShearY(), m.getScaleY())));
        angle = (angle + 360) % 360;
        angles.add(angle);
    }
}

}

