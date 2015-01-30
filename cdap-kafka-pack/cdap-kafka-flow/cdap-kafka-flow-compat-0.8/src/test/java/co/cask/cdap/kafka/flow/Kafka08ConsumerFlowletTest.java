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

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit-test for consuming data in Flow from Kafka 0.8.
 */
public class Kafka08ConsumerFlowletTest extends KafkaConsumerFlowletTestBase {

  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  @BeforeClass
  public static void setup() throws IOException {
    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();
  }

  @AfterClass
  public static void tearDown() {
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
  }

  @Override
  protected Class<? extends KafkaConsumingApp> getApplication() {
    return Kafka08ConsumingApp.class;
  }

  @Override
  protected void sendMessage(String topic, Map<String, String> messages) {
    // Publish a message to Kafka, the flow should consume it
    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);

    // If publish failed, retry up to 20 times, with 100ms delay between each retry
    // This is because leader election in Kafka 08 takes time when a topic is being created upon publish request.
    int count = 0;
    do {
      KafkaPublisher.Preparer preparer = publisher.prepare(topic);
      for (Map.Entry<String, String> entry : messages.entrySet()) {
        preparer.add(Charsets.UTF_8.encode(entry.getValue()), entry.getKey());
      }
      try {
        preparer.send().get();
        break;
      } catch (Exception e) {
        // Backoff if send failed.
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    } while (count++ < 20);
  }

  @Override
  protected boolean supportBrokerList() {
    return true;
  }
}
