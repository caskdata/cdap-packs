/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.metrics.Metrics;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Pulls tweets from the Twitter Stream.
 *
 * <p>
 *   Unless {@link #ARG_TWITTER4J_DISABLED} argument with positive value is provided, this flowlet pulls data from
 *   the twitter stream using twitter4j library. The pulling uses twitter4j.properties file as a configuration, if it is
 *   available in the classpath. Alternatively, user can provide the following auth config options as runtime arguments
 *   for this flowlet:
 *   <ul>
 *     <li>
 *       {@link #ARG_TWITTER4J_OAUTH_CONSUMER_KEY} which corresponds to "oauth.consumerKey" twitter4j config property
 *     </li>
 *     <li>
 *       {@link #ARG_TWITTER4J_OAUTH_CONSUMER_SECRET} which corresponds to "oauth.consumerSecret" twitter4j config 
 *       property
 *     </li>
 *     <li>
 *       {@link #ARG_TWITTER4J_OAUTH_ACCESS_TOKEN} which corresponds to "oauth.accessToken" twitter4j config property
 *     </li>
 *     <li>
 *       {@link #ARG_TWITTER4J_OAUTH_ACCESS_TOKEN_SECRET} which corresponds to the "oauth.accessTokenSecret" twitter4j
 *       config property
 *     </li>
 *   </ul>
 *   
 *   If both twitter4j.properties and runtime argument define the same property, the value of the runtime argument
 *   overrides the one from the file.
 * </p>
 * 
 * <p>
 *   To make testing simpler, the flowlet can be configured to pull tweets from file by supplying file path with
 *   {@link #ARG_SOURCE_FILE} runtime argument to the flow. The file must have JSON-serialized {@link Tweet} objects
 *   with new-line separator between them.
 * </p>
 * <p>
 *   TODO: ideally, we'd allow testing by attaching a {@link co.cask.cdap.api.data.stream.Stream} to feed data,
 *         but current Flow system would require this fake Stream to be always created, which is not convenient.
 * </p>
 *
 * <p>
 *   Internally this flowlet uses a queue for the tweets that where pulled from Twitter stream or read from file and
 *   ready to be emitted. Pulling from Twitter stream or file happens asynchronously to {@link #collect()} method.
 *   The default maximum queue size is {@link #DEFAULT_INTERNAL_QUEUE_SIZE}. Client code can
 *   override this default by passing different value into appropriate constructor.
 *   Emitting is done in batches with {@link #DEFAULT_EMIT_BATCH_MAX_SIZE} as default max batch size. Client code can
 *   override this default by passing different value into appropriate constructor.
 * </p>
 */
public class TweetCollectorFlowlet extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TweetCollectorFlowlet.class);
  private static final Gson GSON = new Gson();

  public static final String ARG_TWITTER4J_DISABLED = "tweet.collector.source.twitter4j.disabled";
  /** Use file as tweets source. Useful in test scenarios  */
  public static final String ARG_SOURCE_FILE = "tweet.collector.source.file";

  public static final int DEFAULT_INTERNAL_QUEUE_SIZE = 10000;
  public static final int DEFAULT_EMIT_BATCH_MAX_SIZE = 100;

  public static final String ARG_TWITTER4J_OAUTH_CONSUMER_KEY = "twitter4j.oauth.consumerKey";
  public static final String ARG_TWITTER4J_OAUTH_CONSUMER_SECRET = "twitter4j.oauth.consumerSecret";
  public static final String ARG_TWITTER4J_OAUTH_ACCESS_TOKEN = "twitter4j.oauth.accessToken";
  public static final String ARG_TWITTER4J_OAUTH_ACCESS_TOKEN_SECRET = "twitter4j.oauth.accessTokenSecret";

  private String name;

  private Metrics metrics;
  private OutputEmitter<Tweet> output;

  private BlockingQueue<Tweet> queue;

  private FileReaderThread fileReader;
  private TweetPuller twitterStreamPuller;

  private int internalQueueSize;
  private int emitBatchMaxSize;

  public TweetCollectorFlowlet() {
    this (DEFAULT_INTERNAL_QUEUE_SIZE, DEFAULT_EMIT_BATCH_MAX_SIZE);
  }

  @Override
  protected void configure() {
    super.configure();
    if (name != null) {
      setName(name);
    }
  }

  public TweetCollectorFlowlet(String name) {
    this(name, DEFAULT_INTERNAL_QUEUE_SIZE, DEFAULT_EMIT_BATCH_MAX_SIZE);
  }

  public TweetCollectorFlowlet(int internalQueueSize, int emitBatchMaxSize) {
    super();
    this.internalQueueSize = internalQueueSize;
    this.emitBatchMaxSize = emitBatchMaxSize;
  }

  public TweetCollectorFlowlet(String name, int internalQueueSize, int emitBatchMaxSize) {
    this(internalQueueSize, emitBatchMaxSize);
    this.name = name;
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);

    // The flowlet pulls tweets from multiple sources in parallel, in separate threads. Pulled tweets are added to the
    // queue and are emitted from the queue using @Tick method in batches.

    Map<String, String> args = context.getRuntimeArguments();

    boolean pullDisabled = false;
    if (args.containsKey(ARG_TWITTER4J_DISABLED)) {
      if (Boolean.parseBoolean(args.get(ARG_TWITTER4J_DISABLED))) {
        LOG.warn("Pulling tweets is disabled via '" + ARG_TWITTER4J_DISABLED + "' runtime argument.");
        pullDisabled = true;
      }
    }

    // todo: allow to use Dataset as a source
    String srcFile = null;
    if (args.containsKey(ARG_SOURCE_FILE)) {
      srcFile = args.get(ARG_SOURCE_FILE);
      LOG.info("Will read tweets from file: " + srcFile);
    }

    // NOTE: we need to create queue before starting threads that write to it
    if (!pullDisabled || srcFile != null) {
      queue = new LinkedBlockingQueue<Tweet>(internalQueueSize);
    }

    if (!pullDisabled) {
      twitterStreamPuller = createTwitterStreamPuller(args);
      twitterStreamPuller.start();
    }

    if (srcFile != null) {
      fileReader = new FileReaderThread(srcFile);
      fileReader.start();
    }
  }

  private TweetPuller createTwitterStreamPuller(Map<String, String> args) {
    // NOTE: oath keys are loaded from twitter4j.properties file, but can be overridden with runtime arguments
    ConfigurationBuilder cb = new ConfigurationBuilder();

    // Override twitter4j.properties file, if provided in runtime args.
    // Ideally, we'd override all, but twitter4j APIs are bad not allowing set arbitrary property...
    if (args.containsKey(ARG_TWITTER4J_OAUTH_CONSUMER_KEY)) {
      cb.setOAuthConsumerKey(args.get(ARG_TWITTER4J_OAUTH_CONSUMER_KEY));
    }
    if (args.containsKey(ARG_TWITTER4J_OAUTH_CONSUMER_SECRET)) {
      cb.setOAuthConsumerSecret(args.get(ARG_TWITTER4J_OAUTH_CONSUMER_SECRET));
    }
    if (args.containsKey(ARG_TWITTER4J_OAUTH_ACCESS_TOKEN)) {
      cb.setOAuthAccessToken(args.get(ARG_TWITTER4J_OAUTH_ACCESS_TOKEN));
    }
    if (args.containsKey(ARG_TWITTER4J_OAUTH_ACCESS_TOKEN_SECRET)) {
      cb.setOAuthAccessTokenSecret(args.get(ARG_TWITTER4J_OAUTH_ACCESS_TOKEN_SECRET));
    }

    Configuration conf = cb.build();

    return new TweetPuller(conf);
  }

  @Override
  public void destroy() {
    if (twitterStreamPuller != null) {
      twitterStreamPuller.stop();
    }
    if (fileReader != null) {
      fileReader.interrupt();
    }
  }

  @Tick(unit = TimeUnit.NANOSECONDS, delay = 0)
  public void collect() throws InterruptedException {
    if (queue == null) {
      // Sleep and return: flowlet is configured with no tweets source
      Thread.sleep(1000);
      return;
    }

    for (int i = 0; i < emitBatchMaxSize; i++) {
      Tweet tweet = queue.poll();
      if (tweet == null) {
        break;
      }

      metrics.count("tweet.collector.out.total", 1);
      emit(tweet);
    }
  }

  /**
   * Emits consumed tweet.
   *
   * By default tweet is emitted into the flowlet queue. Override this method to change this behavior.
   * Note that due to current Flow limitations even if overridden method doesn't emit anything this Flowlet has to be
   * connected to the consumer Flowlet as it defines {@link OutputEmitter} field. The recommended approach is to
   * call super from overridden method.
   *
   * @param tweet tweet to be emitted.
   */
  protected void emit(Tweet tweet) {
    output.emit(tweet);
  }

  private class TweetPuller extends AbstractExecutionThreadService {
    private final Configuration twitter4jConf;

    private TwitterStream twitterStream;
    private final CountDownLatch latch;

    private TweetPuller(Configuration twitter4jConf) {
      this.twitter4jConf = twitter4jConf;
      this.latch = new CountDownLatch(1);
    }

    @Override
    protected void triggerShutdown() {
      latch.countDown();
    }

    @Override
    protected void shutDown() throws Exception {
      twitterStream.cleanUp();
      twitterStream.shutdown();
    }

    @Override
    public void run() {
      LOG.info("Starting pulling from Twitter stream...");

      twitterStream = new TwitterStreamFactory(twitter4jConf).getInstance();

      StatusListener listener = new StatusAdapter() {
        @Override
        public void onStatus(Status status) {
          String text = status.getText();
          try {
            boolean added = false;
            while (!added && latch.getCount() > 0) {
              added = queue.offer(new Tweet(text, status.getCreatedAt().getTime()), 1, TimeUnit.SECONDS);
            }
          } catch (InterruptedException e) {
            LOG.warn("Interrupted while writing to a queue", e);
            Thread.currentThread().interrupt();
          }
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
          LOG.error("Got track limitation notice:" + numberOfLimitedStatuses);
        }

        @Override
        public void onException(Exception ex) {
          LOG.warn("Error during reading from stream" + ex.getMessage());
        }
      };

      twitterStream.addListener(listener);
      twitterStream.sample();
      try {
        latch.await();
      } catch (InterruptedException e) {
        // exiting
        Thread.currentThread().interrupt();
      }
      LOG.info("CollectingThread run() is exiting");
    }

  }

  private class FileReaderThread extends Thread {
    private final String srcFile;

    private FileReaderThread(String srcFile) {
      super("FileReaderThread");
      this.srcFile = srcFile;
    }

    @Override
    public void run() {
      try {
        Files.readLines(new File(srcFile), Charsets.UTF_8, new LineProcessor<Object>() {
          @Override
          public boolean processLine(String tweetJson) throws IOException {
            try {
              Tweet tweet = GSON.fromJson(tweetJson, Tweet.class);
              queue.put(tweet);
            } catch (InterruptedException e) {
              LOG.warn("Interrupted while writing to a queue", e);
              Thread.currentThread().interrupt();
              return false;
            }
            return true;
          }

          @Override
          public Object getResult() {
            // nothing to do here
            return null;
          }
        });
      } catch (IOException e) {
        LOG.error("Failed to read tweets from file " + srcFile, e);
      }
      LOG.info("FileReaderThread run() is exiting");
    }
  }
}
