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
package org.apache.nutch.urlfilter.path;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JUnit test for <code>PathURLFilter</code>.
 *
 */
public class TestPathURLFilter extends TestCase {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  // We have a config here that rejects a last path = /reject
  private static final String paths =
      "# this is a comment\n" +
      "\n" +
      "reject\n" +
      "\n";

  private static final String[] urls = new String[] {
      "https://www.example.com/section/reject",
      "https://www.example.com/reject",
      "https://www.example.com/section/reject?result=this-is-rejected",
      "https://www.example.com/reject/?result=this-is-rejected",
      "https://www.example.com/reject/someotherlastpath",
      "https://www.example.com//ok//reject/",  // test empty paths - reject case
      "https://www.example.com?result=this-is-accepted",
      "https://www.example.com/rej",  // No reject, diff length
      "https://www.example.com/rejection",  // No reject, diff length
      "https://www.example.com//rej//"  // test empty paths - accept case
  };

  private static String[] urlsModeReject = new String[] {
    null,
    null,
    null,
    null,
    null,
    null,
    urls[6],
    urls[7],
    urls[8],
    urls[9]
  };

  private PathURLFilter filter = null;

  public static Test suite() {
    return new TestSuite(TestPathURLFilter.class);
  }

  public static void main(String[] args) {
    TestRunner.run(suite());
  }

  public void setUp() throws IOException {
    filter = new PathURLFilter(paths);
  }

  public void testModeReject() {
    for (int i = 0; i < urls.length; i++) {
      LOG.info("Checking URL:{}", urls[i]);
      assertTrue(urlsModeReject[i] == filter.filter(urls[i]));
    }
  }
}
