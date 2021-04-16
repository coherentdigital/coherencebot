/**
 */
package org.apache.nutch.indexer.summary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.Iterator;

public class TestTextSummarizer {

  Configuration conf = NutchConfiguration.create();
  TextSummarizer summarizer = new TextSummarizer();
  Metadata metadata = new Metadata();

  String testText = 
  "INTRODUCTION Many people and scholars are confused about the difference between jihad and fighting. "
+ "Every time the word jihad is mentioned it is misunderstood and thought to mean fighting or engaging in battle. "
+ "In fact, jihad has a broader and more comprehensive meaning than simply fighting, which is only one type of jihad. "
+ "The comprehensive meaning of jihad extends to spending one's wealth, to jihad by the word, internal jihad, and so on. "
+ "However, in the modern era, the notion of jihad has lost its jurisprudential relevance and instead given rise to an ideological and political discourse.ii The misunderstanding that Islam promotes war and sets no limit in means and methods of armed combat is illusive. "
+ "Islam as a religion of peace abhors aggression and made armed combat a legitimate phenomenon only when it becomes necessary. "
+ "Even in cases where Islam approves armed combat as a legitimate option and allows Muslims to participate in the hostilities, it has at the same time provided for rules to regulate the conduct of the war. "
+ "The words “terrorism” and “terrorist” have become commonplace in the media and in the speeches of politicians since 11 September 2001. "
+ "Frequently these words are preceded by the adjective “Islamic”. "
+ "The uninformed reader might believe that terrorism is something new and something which is essentially and exclusively associated with Islam. "
+ "In Arabic, the translation of, holy war” is not jihad but ‘harb muqaddas’, a term which does not exist in any form in the Islamic tradition. "
+ "Jihad, both linguistically and as a technical term, means “struggle”, and is etymologically related to the words mujahadah, which also means struggle or contention, and ijtihad, which is the effort exerted by jurists to arrive at correct judgments in Islamic law"
+ "iii The Arabic term 2 AN ATTEMPT TO DEFINE JIHAD: TERRORISM AS A FOCAL POINT jihad has been misused due to misconception, manipulation or distortion of its true meaning. "
+ "Linguistically, the term jihad is derived from the Arabic word ‘jahd’, which means fatigue, or from the Arabic word ‘juhd’, which means effort. "
+ "Thus, the term jihad literally means to strive, or to exert one’s efforts, or to earnestly work towards a desired goal or to prevent an undesired one. "
+ "In other words, it is an effort (which makes one feel fatigued) that aims at bringing about benefit or preventing harm. "
+ "In the holy Qur’an the term “jihad” is basically used for an all-out struggle for a certain cause. "
+ "“Holy war” is actually a term that comes out of Christianity. "
+ "Until its acceptance by the Emperor Constantine in the fourth century, Christianity was a minority religion that was often persecuted, and which grew only through preaching and missionary activity. "
+ "Christians were in no position to make war, and indeed Christ’s teachings to turn the other cheek kept them from retaliation against their persecutors in most cases. "
+ "When Christians came to possess real military power, however, they were faced with the task of fighting wars and of deciding when, if ever, a Christian could fight in a war and still be considered a true follower of Christ. "
+ "Augustine was one of the earliest of Church thinkers to address this question in detail, discussing it under the general rubric of “just war”. "
+ "Both he and his mentor Ambrose of Milan described situations in which justice would compel a Christian to take up arms, but without forgetting that war should only be seen as a necessary evil and that it should be stopped once peace is achieved. "
+ "Such ideas were later elaborated upon by such figures as Thomas Aquinas and Hugo Grotius. "
+ "It was with the rise of the Papal States and ultimately with the declaration of the Crusades that the concept of “holy war” came to be an important term. "
+ "It is noteworthy that the earliest “holy wars” were often wars by Christians against other Christians, in the sense that the protagonists saw themselves as carrying out the will of God. "
+ "However, it was with the “taking of the cross” by the Christian warrior sent by Pope Urban in the eleventh century that “just war” became “holy war” in its fullest sense. "
+ "It was only with the authorization of the Pope that a knight could adopt the symbol of the cross. "
+ "“Holy war”, as a term, thus has its origins in Christianity, not Islam.iv The term “holy war” is thus inaccurate and unhelpful; implying that for Muslims war has a kind of supernatural and unreasoned quality removed from the exigencies of the world. "
+ "On the contrary, Islamic law treats war as sometimes a necessary evil, whose conduct is constrained by concrete goals of justice and fairness in this world. "
+ "v 3 AN ATTEMPT TO DEFINE JIHAD: TERRORISM AS A FOCAL POINT JIHAD IN THE ISLAMIC LAW PERSPECTIVE First, we should have look at the concept of jihad, which is usually mistranslated in the West as “holy war”. "
+ "The term jihad comes from the Arab verb “jahada”, meaning to struggle or exert";


  @Before
  public void setUp() throws Exception {
    metadata.add(Response.CONTENT_TYPE, "text/html");
  }

  @Test
  public void testSummaryIsAdded() throws Exception {
    summarizer.setConf(conf);

    Outlink[] outlinks = new Outlink[5];

    NutchDocument doc = summarizer.filter(
      new NutchDocument(),
      new ParseImpl(testText, new ParseData(new ParseStatus(), "title", outlinks, metadata)),
      new Text("https://www.example.com"),
      new CrawlDatum(),
      new Inlinks()
    );

    Assert.assertEquals(1, doc.getField("summary").getValues().size());
  }
}
