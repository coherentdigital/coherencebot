/**
 * Coherent Digital Text Summarizer.
 */
package net.coherentdigital.summary;

import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * An POJO that computes a summary field of its text input.
 */
public class TextSummarizer {

  public final static org.slf4j.Logger LOG = LoggerFactory
      .getLogger(TextSummarizer.class);

  private final static int MAX_SUMMARY_LENGTH = 5;

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

  /**
   * Command line class to summarize a text file.
   *
   * @param argv path to file.txt
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {

    String usage = "java --cp text-summarizer.jar net.coherentdigital.summary.TextSummarizer {file} {sentences}";
    String args = "Input is a txt file.  Sentences is optional, defaults to 5";
    int count = MAX_SUMMARY_LENGTH;

    if (argv.length < 1) {
      System.out.println("usage:");
      System.out.println(usage);
      System.out.println(args);
      return;
    }

    String content = new String(Files.readAllBytes(Paths.get(argv[0])));

    if (argv.length > 1) {
      try {
        count = Integer.parseInt(argv[1]);
      } catch (Exception e) {
        System.err.println("Second arg does not parse as integer");
      }
    }

    SummaryTool summaryTool = new SummaryTool(content.replaceAll("�", ""));
    String summary = summaryTool.createSummary(count);
    TextSummarizer ts = new TextSummarizer();

    if (summary.length() > 0) {
      summary = ts.clean(summary);
      System.out.println(summary);
    }
  }
}
