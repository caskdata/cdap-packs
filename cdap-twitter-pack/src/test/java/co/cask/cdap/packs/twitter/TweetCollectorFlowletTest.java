/*
 * Copyright 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.packs.twitter;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test for TweetCollectorFlowlet.
 */
public class TweetCollectorFlowletTest extends TestBase {

  @Test
  public void test() throws Exception {
    Set<Tweet> tweets = ImmutableSet.of(
      new Tweet("tweet1", 1000),
      new Tweet("tweet2", 2000)
    );
    File srcFile = TweetCollectorTestUtil.writeToTempFile(tweets.iterator());

    ApplicationManager appManager = deployApplication(TweetCollectorApp.class);
    FlowManager flowManager = appManager.getFlowManager("TweetCollectorFlow");
    flowManager.start(ImmutableMap.of(TweetCollectorFlowlet.ARG_TWITTER4J_DISABLED, "true",
                                      TweetCollectorFlowlet.ARG_SOURCE_FILE, srcFile.getPath()));

    RuntimeMetrics countMetrics = flowManager.getFlowletMetrics("persistor");
    countMetrics.waitForProcessed(2, 2, TimeUnit.SECONDS);

    DataSetManager<ObjectStore<Tweet>> tweetsDataset = getDataset("tweets");
    CloseableIterator<KeyValue<byte[], Tweet>> scan = tweetsDataset.get().scan(null, null);
    Set<Tweet> result = Sets.newHashSet();
    while (scan.hasNext()) {
      result.add(scan.next().getValue());
    }
    Assert.assertEquals(tweets, result);
  }
}
