/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Unit-test for Kafka consuming flowlet for Kafka 0.7.
 */
public class Kafka07ConsumerFlowletTest extends KafkaConsumerFlowletTestBase {

  private static ZKClientService zkClient;
  private static EmbeddedKafkaServer kafkaServer;

  @BeforeClass
  public static void setup() throws IOException {
    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(), kafkaPort,
                                                              TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();
    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();
  }

  @AfterClass
  public static void tearDown() {
    zkClient.stopAndWait();
    kafkaServer.stopAndWait();
  }

  @Override
  protected Class<? extends KafkaConsumingApp> getApplication() {
    return Kafka07ConsumingApp.class;
  }

  @Override
  protected void sendMessage(String topic, Map<String, String> messages) {
    Properties prop = new Properties();
    prop.setProperty("zk.connect", zkServer.getConnectionStr());
    prop.setProperty("serializer.class", "kafka.serializer.StringEncoder");

    ProducerConfig prodConfig = new ProducerConfig(prop);

    List<ProducerData<String, String>> outMessages = new ArrayList<>();
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      outMessages.add(new ProducerData<>(topic, entry.getKey(), ImmutableList.of(entry.getValue())));
    }
    Producer<String, String> producer = new Producer<>(prodConfig);
    producer.send(outMessages);
  }

  @Override
  protected boolean supportBrokerList() {
    return false;
  }
}
