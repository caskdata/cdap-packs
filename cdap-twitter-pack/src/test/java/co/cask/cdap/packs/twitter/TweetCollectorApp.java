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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;

import java.util.UUID;

/**
 * App for testing {@link TweetCollectorFlowlet}.
 */
public class TweetCollectorApp extends AbstractApplication {
  @Override
  public void configure() {
    addFlow(new TweetCollectorFlow());
    try {
      ObjectStores.createObjectStore(getConfigurer(), "tweets", Tweet.class);
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("Will never happen", e);
    }
  }

  static final class TweetCollectorFlow extends AbstractFlow {
    @Override
    public void configure() {
      setName("TweetCollectorFlow");
      setDescription("Flow to test TweetCollectorFlowlet");
      addFlowlet("collector", new TweetCollectorFlowlet());
      addFlowlet("persistor", new TweetPersistorFlowlet());
      connect("collector", "persistor");
    }
  }

  static final class TweetPersistorFlowlet extends AbstractFlowlet {
    @UseDataSet("tweets")
    private ObjectStore<Tweet> tweets;

    @ProcessInput
    public void process(Tweet tweet) {
      tweets.write(UUID.randomUUID().toString(), tweet);
    }
  }
}
