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

package co.cask.cdap.kafka.flow;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.tephra.TxConstants;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Abstract base class for writing a Kafka consuming flowlet test.
 */
public abstract class KafkaConsumerFlowletTestBase extends TestBase {

  @ClassRule
  public static final TestConfiguration CONF = new TestConfiguration("explore.enabled", false);

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerFlowletTestBase.class);
  private static final int PARTITIONS = 6;

  // the tx timeout needs to be this small value in order to test processing limits (#testProcessingLimits)
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(TxConstants.Manager.CFG_TX_TIMEOUT, "2");

  static InMemoryZKServer zkServer;
  static int kafkaPort;

  @BeforeClass
  public static void init() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void cleanup() {
    zkServer.stopAndWait();
  }

  @After
  public void cleanUpMetrics() throws Exception {
    getMetricsManager().resetAll();
    clear();
  }

  /**
   * Returns the {@link Application} class for running the test.
   */
  protected abstract Class<? extends KafkaConsumingApp> getApplication();

  /**
   * Publish messages to Kafka for the given topic.
   *
   * @param topic Topic to publish to
   * @param message Map from message key to payload
   */
  protected abstract void sendMessage(String topic, Map<String, String> message);

  protected abstract boolean supportBrokerList();

  /**
   * Returns a Map supplied as runtime arguments to the Flow started by this test.
   */
  private Map<String, String> getRuntimeArgs(String topic, int partitions, boolean preferZK) {
    Map<String, String> args = Maps.newHashMap();

    args.put("kafka.topic", topic);
    if (!supportBrokerList() || preferZK) {
      args.put("kafka.zookeeper", zkServer.getConnectionStr());
    } else {
      args.put("kafka.brokers", "localhost:" + kafkaPort);
    }
    args.put("kafka.partitions", Integer.toString(partitions));
    return args;
  }

  protected Map<String, String> getRuntimeArgs(String topic, int partitions, boolean preferZK, long startOffset) {
    Map<String, String> args = getRuntimeArgs(topic, partitions, preferZK);
    args.put("kafka.default.offset", Long.toString(startOffset));
    return args;
  }

  @Test
  public final void testFlowlet() throws Exception {
    String topic = "testTopic";

    ApplicationManager appManager = deployApplication(getApplication());
    FlowManager flowManager =
      appManager.getFlowManager("KafkaConsumingFlow").start(getRuntimeArgs(topic, PARTITIONS, false));

    // Publish 5 messages to Kafka, the flow should consume them
    int msgCount = 5;
    Map<String, String> messages = Maps.newHashMap();
    for (int i = 0; i < msgCount; i++) {
      messages.put(Integer.toString(i), "Message " + i);
    }
    sendMessage(topic, messages);

    RuntimeMetrics sinkMetrics = getMetricsManager().getFlowletMetrics(
      Id.Namespace.DEFAULT.getId(), "KafkaConsumingApp", "KafkaConsumingFlow", "DataSink");
    sinkMetrics.waitForProcessed(msgCount, 10, TimeUnit.SECONDS);

    // Sleep for a while; no more messages should be processed
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(msgCount, sinkMetrics.getProcessed());

    flowManager.stop();

    // Publish a message, a failure and another message when the flow is not running
    messages.clear();
    messages.put(Integer.toString(msgCount), "Message " + msgCount++);
    messages.put("Failure", "Failure");
    messages.put(Integer.toString(msgCount), "Message " + msgCount++);
    sendMessage(topic, messages);

    // Clear stats and start the flow again (using ZK to discover broker this time)
    getMetricsManager().resetAll();
    flowManager = startFlowWithRetry(appManager, "KafkaConsumingFlow", getRuntimeArgs(topic, PARTITIONS, true), 5);

    // Wait for 2 messages. This should be ok.
    sinkMetrics.waitForProcessed(2L, 10, TimeUnit.SECONDS);

    // Sleep for a while; no more messages should be processed
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(2, sinkMetrics.getProcessed());

    flowManager.stop();
    assertDatasetCount(msgCount);
  }

  @Test
  public void testChangeInstances() throws Exception {
    // Start the flow with one instance source.
    String topic = "testChangeInstances";
    ApplicationManager appManager = deployApplication(getApplication());
    FlowManager flowManager =
      appManager.getFlowManager("KafkaConsumingFlow").start(getRuntimeArgs(topic, PARTITIONS, false));

    // Publish 100 messages. Expect each partition to get some of the messages.
    int msgCount = 100;
    for (int i = 0; i < msgCount; i++) {
      sendMessage(topic, ImmutableMap.of(Integer.toString(i), "TestInstances " + i));
    }

    // Should received 100 messages
    RuntimeMetrics sinkMetrics = getMetricsManager().getFlowletMetrics(
      Id.Namespace.DEFAULT.getId(), "KafkaConsumingApp", "KafkaConsumingFlow", "DataSink");
    sinkMetrics.waitForProcessed(msgCount, 10, TimeUnit.SECONDS);

    // Scale to 3 instances
    flowManager.setFlowletInstances("KafkaSource", 3);

    // Send another 100 messages
    for (int i = 0; i < msgCount; i++) {
      sendMessage(topic, ImmutableMap.of(Integer.toString(i + msgCount), "TestInstances " + (i + msgCount)));
    }
    msgCount *= 2;

    // Should have received another 100 messages
    sinkMetrics.waitForProcessed(msgCount, 10, TimeUnit.SECONDS);

    flowManager.stop();
    assertDatasetCount(msgCount);
  }

  @Test
  public final void testStartOffset() throws Exception {
    String topic = "testStartOffset";
    // Publish 5 messages to Kafka before starting the flow.
    int msgCount = 5;
    Map<String, String> messages = Maps.newHashMap();
    for (int i = 0; i < msgCount; i++) {
      messages.put(Integer.toString(i), "Message " + i);
    }
    sendMessage(topic, messages);

    ApplicationManager appManager = deployApplication(getApplication());
    // -1 as the beginOffset signals that the flow should start reading from the last event currently in kafka (so it
    // should ignore the 5 that were sent before starting the flow.
    FlowManager flowManager =
      appManager.getFlowManager("KafkaConsumingFlow").start(getRuntimeArgs(topic, PARTITIONS, false, -1));
    // Give the flow some time to startup and initialize. It needs some time even after FlowManager#isRunning is true.
    TimeUnit.SECONDS.sleep(2);

    // Publish an additional 5 messages to Kafka, the flow should consume them
    messages = Maps.newHashMap();
    for (int i = msgCount; i < msgCount + 5; i++) {
      messages.put(Integer.toString(i), "Message " + i);
    }
    sendMessage(topic, messages);

    RuntimeMetrics sinkMetrics = getMetricsManager().getFlowletMetrics(
      Id.Namespace.DEFAULT.getId(), "KafkaConsumingApp", "KafkaConsumingFlow", "DataSink");
    sinkMetrics.waitForProcessed(5, 10, TimeUnit.SECONDS);

    // Sleep for a while; Even though we sent 10 messages total, we sent only 5 after starting the flow, so
    // no more messages should be processed
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(msgCount, sinkMetrics.getProcessed());

    flowManager.stop();
    assertDatasetCount(msgCount);
  }

  @Test
  public final void testInvalidStartOffsetLarger() throws Exception {
    String topic = "testInvalidStartOffsetLarger";
    // Publish 5 messages to Kafka before starting the flow.
    int msgCount = 5;
    Map<String, String> messages = Maps.newHashMap();
    for (int i = 0; i < msgCount; i++) {
      messages.put(Integer.toString(i), "Message " + i);
    }
    sendMessage(topic, messages);

    ApplicationManager appManager = deployApplication(getApplication());
    // Invalid start offset as the beginOffset throws OffsetOutOfRangeException on trying to fetch messages from Kafka.
    // Since the invalid offset is larger than the latest offset available, the flow should start reading from the 
    // last message currently in kafka (so it should ignore the 5 that were sent before starting the flow).
    long invalidStartOffset = 12345678901234L;
    FlowManager flowManager =
      appManager.getFlowManager("KafkaConsumingFlow")
        .start(getRuntimeArgs(topic, PARTITIONS, false, invalidStartOffset));
    // Give the flow some time to startup and initialize. It needs some time even after FlowManager#isRunning is true.
    TimeUnit.SECONDS.sleep(2);

    // Publish an additional 5 messages to Kafka, the flow should consume them
    messages = Maps.newHashMap();
    for (int i = msgCount; i < msgCount + 5; i++) {
      messages.put(Integer.toString(i), "Message " + i);
    }
    sendMessage(topic, messages);

    RuntimeMetrics sinkMetrics = getMetricsManager().getFlowletMetrics(
      Id.Namespace.DEFAULT.getId(), "KafkaConsumingApp", "KafkaConsumingFlow", "DataSink");
    sinkMetrics.waitForProcessed(5, 10, TimeUnit.SECONDS);

    // Sleep for a while; Even though we sent 10 messages total, we sent only 5 after starting the flow, so
    // no more messages should be processed
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(msgCount, sinkMetrics.getProcessed());

    flowManager.stop();
    assertDatasetCount(msgCount);
  }

  @Test
  public final void testInvalidStartOffsetSmaller() throws Exception {
    String topic = "testInvalidStartOffsetSmaller";
    // Publish 500 messages to Kafka before starting the flow.
    int msgCount = 500;
    Map<String, String> messages = Maps.newHashMap();
    for (int i = 0; i < msgCount; i++) {
      messages.put(Integer.toString(i), "Test Invalid Start Offset Message " + i);
    }
    sendMessage(topic, messages);

    messages.clear();
    for (int i = msgCount; i < 2 * msgCount; i++) {
      messages.put(Integer.toString(i), "Test Invalid Start Offset Message " + i);
    }
    sendMessage(topic, messages);

    // Wait for more than one minute for some log segments to get deleted.
    TimeUnit.SECONDS.sleep(80);

    ApplicationManager appManager = deployApplication(getApplication());

    // Setup expected count by reading from earliest offset available
    FlowManager flowManager =
      appManager.getFlowManager("KafkaConsumingFlow").start(getRuntimeArgs(topic, PARTITIONS, false, -2));
    // Give the flow some time to startup and initialize. It needs some time even after FlowManager#isRunning is true.
    TimeUnit.SECONDS.sleep(2);

    RuntimeMetrics sinkMetrics = getMetricsManager().getFlowletMetrics(
      Id.Namespace.DEFAULT.getId(), "KafkaConsumingApp", "KafkaConsumingFlow", "DataSink");
    // We don't know exactly how many messages to wait for, waiting for the minimum messages we know we'll fetch.
    sinkMetrics.waitForProcessed(10, 30, TimeUnit.SECONDS);

    // Sleep for a while to fetch all messages, since we don't exactly know how many messages we are supposed to fetch.
    TimeUnit.SECONDS.sleep(10);
    flowManager.stop();

    long expectedCount = sinkMetrics.getProcessed();
    LOG.info("Fetched {} messages from Kafka", expectedCount);
    // Make sure fetched at least one message from Kafka.
    Assert.assertTrue(expectedCount > 1);
    // Also, make sure some messages got deleted in Kafka.
    Assert.assertTrue(expectedCount < 2 * msgCount);

    // Clear everything, and read again with invalid offset
    clear();

    appManager = deployApplication(getApplication());
    // Invalid start offset as the beginOffset throws OffsetOutOfRangeException on trying to fetch messages from Kafka.
    // Since the invalid offset is smaller than the latest offset available, the flow should start reading from the
    // earliest message currently in kafka. The total number of events read should be equal to expectedCount
    // calculated above.
    long invalidStartOffset = 0L;
    flowManager =
      appManager.getFlowManager("KafkaConsumingFlow")
        .start(getRuntimeArgs(topic, PARTITIONS, false, invalidStartOffset));
    // Give the flow some time to startup and initialize. It needs some time even after FlowManager#isRunning is true.
    TimeUnit.SECONDS.sleep(2);

    sinkMetrics = getMetricsManager().getFlowletMetrics(
      Id.Namespace.DEFAULT.getId(), "KafkaConsumingApp", "KafkaConsumingFlow", "DataSink");
    // We don't know exactly how many messages to wait for, waiting for the minimum messages we know we'll fetch.
    sinkMetrics.waitForProcessed(expectedCount, 30, TimeUnit.SECONDS);

    // Sleep for a while to fetch all messages
    TimeUnit.SECONDS.sleep(2);
    flowManager.stop();
    Assert.assertEquals(expectedCount, sinkMetrics.getProcessed());
    assertDatasetCount(expectedCount);
  }

  @Test
  public void testProcessingLimits() throws Exception {
    String topic = "testProcessingLimits";

    ApplicationManager appManager = deployApplication(getApplication());
    FlowManager flowManager = appManager.getFlowManager("KafkaConsumingFlow");

    // running the flow with small batch size or processing duration allows the flow to complete within the transaction
    // timeout
    Map<String, String> runtimeArgs = getRuntimeArgs(topic, PARTITIONS, false);
    runtimeArgs.put("batch.size", "10");
    runFlow(flowManager, runtimeArgs, topic);

    runtimeArgs = getRuntimeArgs(topic, PARTITIONS, false);
    runtimeArgs.put("max.process.millis", "2000");
    runFlow(flowManager, runtimeArgs, topic);

    // without specifying batch size or max process duration, transaction timeout is encountered and so the events
    // are not processed
    runtimeArgs = getRuntimeArgs(topic, PARTITIONS, false);
    try {
      runFlow(flowManager, runtimeArgs, topic);
      Assert.fail();
    } catch (TimeoutException e) {
      // expected
    }
  }

  // helper for #testProcessingLimits
  private void runFlow(FlowManager flowManager, Map<String, String> args, String topic) throws Exception {
    // Publish 100 messages to Kafka, the flow should consume them
    int msgCount = 100;
    Map<String, String> messages = Maps.newHashMap();
    for (int i = 0; i < msgCount; i++) {
      messages.put(Integer.toString(i), "sleep:100");
    }
    sendMessage(topic, messages);

    flowManager.start(args);
    try {
      RuntimeMetrics sinkMetrics = getMetricsManager().getFlowletMetrics(
        Id.Namespace.DEFAULT.getId(), "KafkaConsumingApp", "KafkaConsumingFlow", "DataSink");
      sinkMetrics.waitForProcessed(msgCount, 15, TimeUnit.SECONDS);
      // should be no exceptions/retries
      RuntimeMetrics sourceMetrics = getMetricsManager().getFlowletMetrics(
        Id.Namespace.DEFAULT.getId(), "KafkaConsumingApp", "KafkaConsumingFlow", "KafkaSource");
      Assert.assertEquals(0, sourceMetrics.getException());
    } finally {
      flowManager.stop();
      getMetricsManager().resetAll();
    }
  }

  private void assertDatasetCount(long expectedMsgCount) throws Exception {
    // Verify using the Dataset counter table; it keeps a count for each message that the sink flowlet received
    DataSetManager<KeyValueTable> datasetManager = getDataset("counter");
    KeyValueTable counter = datasetManager.get();
    CloseableIterator<KeyValue<byte[], byte[]>> scanner = counter.scan(null, null);
    try {
      int size = 0;
      while (scanner.hasNext()) {
        KeyValue<byte[], byte[]> keyValue = scanner.next();
        Assert.assertEquals(1L, Bytes.toLong(keyValue.getValue()));
        size++;
      }
      Assert.assertEquals(expectedMsgCount, size);
    } finally {
      scanner.close();
    }
  }

  private FlowManager startFlowWithRetry(ApplicationManager appManager,
                                         String flowId, Map<String, String> args, int trials) {
    Throwable failure = null;
    do {
      try {
        if (failure != null) {
          TimeUnit.SECONDS.sleep(1);
        }
        return appManager.getFlowManager(flowId).start(args);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      } catch (Throwable t) {
        // Just memorize the failure
        failure = t;
      }
    } while (--trials > 0);

    throw Throwables.propagate(failure);
  }

  protected static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    // Note: the log size properties below have been set so that we can have log rollovers
    // and log deletions in a minute. 
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", Integer.toString(PARTITIONS));
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "100");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    prop.setProperty("log.retention.bytes", "1000");
    prop.setProperty("log.retention.check.interval.ms", "60000");

    // These are for Kafka-0.7
    prop.setProperty("brokerid", "1");
    prop.setProperty("zk.connect", zkConnectStr);
    prop.setProperty("zk.connectiontimeout.ms", "1000000");
    prop.setProperty("log.retention.size", "1000");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("log.file.size", "1000");

    return prop;
  }
}
