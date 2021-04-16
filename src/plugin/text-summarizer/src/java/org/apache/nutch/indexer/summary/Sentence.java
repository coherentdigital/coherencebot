package org.apache.nutch.indexer.summary;

class Sentence {
  // Reading order
  int number;

  // Strength of this sentence based on common words.
  double score;

  // Words in the sentence
  int noOfWords;

  // Text of the sentence
  String value;

  Sentence(int number, String value){
    this.number = number;
    this.value = new String(value);
    noOfWords = value.split("\\s+").length;
    score = 0.0;
  }
}
