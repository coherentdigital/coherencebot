package net.coherentdigital.summary;

import java.util.ArrayList;
import java.util.Collections;
import org.slf4j.LoggerFactory;

class SummaryTool {
  public final static org.slf4j.Logger LOG = LoggerFactory
      .getLogger(SummaryTool.class);

  private static final int MAX_CANDIDATES = 500;
  private static final int MAX_SENTENCE_LENGTH = 400;
  private static final int MIN_SENTENCE_LENGTH = 20;

  ArrayList<Sentence> sentences, contentSummary;
  int noOfSentences;
  String textToSummarize;

  double[][] intersectionMatrix;

  public SummaryTool(String fullText) {
    if (fullText != null) {
      // In PDF extracts; spaces are often missing between sentences.
      // This adds a space between any period followed by an uppercase letter.
      textToSummarize = fullText.replaceAll("([^\\s]+\\.)(\\p{Lu})", "$1 $2");
      // Mark all the ends of sentences
      textToSummarize = textToSummarize.replaceAll("([^\\s]{2,}[\\.\\?])\\s+", "$1~SENT~");
    }
  }

  private void init() {
    sentences = new ArrayList<Sentence>();
    contentSummary = new ArrayList<Sentence>();
    noOfSentences = 0;
  }

  /**
   * Gets the sentences from the entire passage
   */
  private void extractSentencesFromContext() {
    if (textToSummarize != null && textToSummarize.length() > 0) {
      String[] rawSentences = textToSummarize.split("~SENT~", MAX_CANDIDATES);

      for (int i = 0; i < rawSentences.length
          && i < (MAX_CANDIDATES - 1); i++) {
        String sentence = rawSentences[i];
        if (sentence.length() > MAX_SENTENCE_LENGTH) {
          sentence = sentence.substring(0, MAX_SENTENCE_LENGTH);
        }
        if (sentence.length() < MIN_SENTENCE_LENGTH) {
          continue;
        }
        sentence = sentence.trim();
        sentences.add(new Sentence(noOfSentences, sentence));
        noOfSentences++;
      }
    }
  }

  private double noOfCommonWords(Sentence str1, Sentence str2) {
    double commonCount = 0;

    for (String str1Word : str1.value.split("\\s+")) {
      for (String str2Word : str2.value.split("\\s+")) {
        if (str1Word.compareToIgnoreCase(str2Word) == 0) {
          commonCount++;
        }
      }
    }

    return commonCount;
  }

  /**
   * For each sentence compute a score that is based on the common words shared
   * with other sentences.
   */
  private void createIntersectionMatrix() {
    intersectionMatrix = new double[noOfSentences][noOfSentences];
    for (int i = 0; i < noOfSentences; i++) {
      for (int j = 0; j < noOfSentences; j++) {

        if (i <= j) {
          Sentence str1 = sentences.get(i);
          Sentence str2 = sentences.get(j);
          intersectionMatrix[i][j] = noOfCommonWords(str1, str2)
              / ((double) (str1.noOfWords + str2.noOfWords) / 2);
        } else {
          intersectionMatrix[i][j] = intersectionMatrix[j][i];
        }
      }
    }
  }

  /**
   * Distribute the scores from the intersection matrix to each sentence. This
   * sums the score for each sentence adding all the scores of other sentences
   * it shares words with.
   */
  private void assignScores() {
    for (int i = 0; i < noOfSentences; i++) {
      double score = 0;
      for (int j = 0; j < noOfSentences; j++) {
        score += intersectionMatrix[i][j];
      }
      ((Sentence) sentences.get(i)).score = score;
    }
  }

  /**
   * Create a summary with <code>count</code> sentences.
   */
  public String createSummary(int count) {
    init();
    extractSentencesFromContext();
    createIntersectionMatrix();
    assignScores();

    // Sort based on score (importance).
    Collections.sort(sentences, new SentenceComparator());
    for (int i = 0; i < count && i < sentences.size(); i++) {
      contentSummary.add(sentences.get(i));
    }

    // To ensure summary is in reading order.
    Collections.sort(contentSummary, new SentenceComparatorForSummary());
    String summary = "";
    for (Sentence sentence : contentSummary) {
      if (summary.length() > 0) {
        summary += " [...] ";
      }
      summary += sentence.value;
    }
    return summary.trim();
  }
}
