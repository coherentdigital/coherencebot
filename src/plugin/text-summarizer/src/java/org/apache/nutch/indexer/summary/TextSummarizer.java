/**
 * Coherent Digital Text Summarizer.
 */
package org.apache.nutch.indexer.summary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.util.StringUtil;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * An {@link org.apache.nutch.indexer.IndexingFilter} that adds a
 * <code>summary</code> field to the document by summarizing the document text.
 */
public class TextSummarizer implements IndexingFilter {

  public final static org.slf4j.Logger LOG = LoggerFactory
      .getLogger(TextSummarizer.class);

  private Configuration conf;

  private final static int MAX_SUMMARY_LENGTH = 5;

  @Override
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    // See if the doc already has a summary.
    String summary = "";
    NutchField summaryField = doc.getField("summary");
    if (summaryField == null) {
      summaryField = doc.getField("metatag.description");
    }
    if (summaryField == null) {
      summaryField = doc.getField("metatag.twitter:description");
    }
    if (summaryField != null) {
      List<Object> values = summaryField.getValues();
      for (int i = 0; i < values.size(); i++) {
        summary = summary + values.get(i).toString();
      }
    }

    // If its empty, remove it.
    if (summary.length() == 0) {
      summary = null;
      doc.removeField("summary");
      doc.removeField("metatag.description");
      doc.removeField("metatag.twitter:description");
    }

    // Generate a summary from the text.
    if (summary == null) {
      // The content to summarize
      String content = parse.getText();
      SummaryTool summaryTool = new SummaryTool(StringUtil.cleanField(content));
      summary = summaryTool.createSummary(MAX_SUMMARY_LENGTH);

      if (summary.length() > 0) {
        summary = clean(summary);
        doc.add("summary", summary);
      }
    }

    if (doc.getField("heading") == null) {
      String contentType = parse.getData().getMeta(Response.CONTENT_TYPE);
      if ("application/pdf".equals(contentType)) {
        String content = parse.getText();
        if (content.length() > 4000) {
          SummaryTool summaryTool = new SummaryTool(
              StringUtil.cleanField(content));
          String heading = summaryTool.extractHeading();
          if (heading != null) {
            doc.add("heading", heading);
          }
        }
      }
    }

    // Compute contentLength if not present.
    NutchField contentLengthField = doc.getField("contentLength");
    if (contentLengthField == null && parse.getText().length() > 0) {
      int contentLength = parse.getText().length();
      doc.add("contentLength", Integer.valueOf(contentLength));
    }

    // Compute other field lengths so they can be used in record selection
    // during curation.
    int titleLength = 0;
    if (doc.getField("title") != null) {
      StringBuffer sb = new StringBuffer();
      List<Object> values = doc.getField("title").getValues();
      for (int i = 0; i < values.size(); i++) {
        sb.append(values.get(i).toString());
      }
      titleLength = sb.toString().length();
    }
    doc.add("titleLength", Integer.valueOf(titleLength));

    int headingLength = 0;
    if (doc.getField("heading") != null) {
      StringBuffer sb = new StringBuffer();
      List<Object> values = doc.getField("heading").getValues();
      for (int i = 0; i < values.size(); i++) {
        sb.append(values.get(i).toString());
      }
      headingLength = sb.toString().length();
    }
    doc.add("headingLength", Integer.valueOf(headingLength));

    int anchorLength = 0;
    if (doc.getField("anchor") != null) {
      StringBuffer sb = new StringBuffer();
      List<Object> values = doc.getField("anchor").getValues();
      for (int i = 0; i < values.size(); i++) {
        sb.append(values.get(i).toString());
      }
      anchorLength = sb.toString().length();
    }
    doc.add("anchorLength", Integer.valueOf(anchorLength));

    return doc;
  }

  /**
   * Remove undesirable junk from a text summary.
   *
   * @param text
   * @return
   */
  private String clean(String text) {
    String returnText = text;
    if (text != null && text.length() > 0) {
      // Remove links from summaries
      returnText = returnText.replaceAll("https?://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", " ");
      // Remove non-printable characters from summaries
      returnText = returnText.replaceAll("\\p{C}", " ");
      // Compress white space
      returnText = returnText.replaceAll("\\s+", " ").trim();
    }
    return returnText;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Command line class to summarize a text file.
   *
   * @param argv path to file.txt
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {

    String usage = "java -jar some.jar TextSummarizer <input text file>";

    if (argv.length < 1) {
      System.out.println("usage:" + usage);
      return;
    }

    Path fileName = Path.of(argv[0]);
    String content = Files.readString(fileName);

    SummaryTool summaryTool = new SummaryTool(StringUtil.cleanField(content));
    String summary = summaryTool.createSummary(MAX_SUMMARY_LENGTH);
    TextSummarizer ts = new TextSummarizer();

    if (summary.length() > 0) {
      summary = ts.clean(summary);
      System.out.println(summary);
    }
  }
}
