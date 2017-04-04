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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Application for testing Kafka consuming flowlet for Kafka 0.8.
 */
public class Kafka08ConsumingApp extends KafkaConsumingApp {

  private final Flowlet sourceFlowlet = new KafkaSource();

  @Override
  protected Flowlet getKafkaSourceFlowlet() {
    return sourceFlowlet;
  }

  /**
   * A flowlet to poll from Kafka.
   */
  public static final class KafkaSource extends Kafka08ConsumerFlowlet<byte[], String> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    @UseDataSet("kafkaOffsets")
    private KeyValueTable offsetStore;

    private boolean failed;
    private OutputEmitter<String> emitter;

    protected void processMessage(KafkaMessage<Long> message) throws Exception {
      // Should only receive messages from partitions that it can process
      int partition = message.getTopicPartition().getPartition();
      if ((partition % getContext().getInstanceCount()) != getContext().getInstanceId()) {
        throw new IllegalArgumentException("Received unexpected partition " + partition);
      }

      super.processMessage(message);
    }

    @Override
    protected void processMessage(String value) throws Exception {
      LOG.info("Message: {}", value);
      if (value.equals("Failure")) {
        if (!failed) {
          failed = true;
          // Intentionally throw exception for the first time that it sees a failure message.
          // The second time will ignore it, not emitting to downstream
          throw new IllegalStateException("Failed with value: " + value);
        }
        return;
      } else if (value.startsWith("sleep")) {
        // 'sleep:500'
        TimeUnit.MILLISECONDS.sleep(Integer.parseInt(value.split(":")[1]));
      }
      emitter.emit(value);
    }

    @Override
    protected void configureKafka(KafkaConfigurer configurer) {
      Map<String, String> runtimeArgs = getContext().getRuntimeArguments();
      if (runtimeArgs.containsKey("kafka.zookeeper")) {
        configurer.setZooKeeper(runtimeArgs.get("kafka.zookeeper"));
      } else if (runtimeArgs.containsKey("kafka.brokers")) {
        configurer.setBrokers(runtimeArgs.get("kafka.brokers"));
      }
      setupTopicPartitions(configurer, runtimeArgs);
      if (runtimeArgs.containsKey("batch.size")) {
        configurer.setProcessBatchSize(Integer.parseInt(runtimeArgs.get("batch.size")));
      }
      if (runtimeArgs.containsKey("max.process.millis")) {
        configurer.setMaxProcessTime(Integer.parseInt(runtimeArgs.get("max.process.millis")));
      }
    }

    @Override
    protected void handleInstancesChanged(KafkaConsumerConfigurer configurer) {
      setupTopicPartitions(configurer, getContext().getRuntimeArguments());
    }

    private void setupTopicPartitions(KafkaConsumerConfigurer configurer, Map<String, String> runtimeArgs) {
      int partitions = Integer.parseInt(runtimeArgs.get("kafka.partitions"));
      int instanceId = getContext().getInstanceId();
      int instances = getContext().getInstanceCount();
      for (int i = 0; i < partitions; i++) {
        if ((i % instances) == instanceId) {
          configurer.addTopicPartition(runtimeArgs.get("kafka.topic"), i);
        }
      }
    }

    @Override
    protected long getDefaultOffset(TopicPartition topicPartition) {
      String argValue = getContext().getRuntimeArguments().get("kafka.default.offset");
      if (argValue != null) {
        return Long.valueOf(argValue);
      }

      return super.getDefaultOffset(topicPartition);
    }

    @Override
    protected KeyValueTable getOffsetStore() {
      return offsetStore;
    }
  }
}
