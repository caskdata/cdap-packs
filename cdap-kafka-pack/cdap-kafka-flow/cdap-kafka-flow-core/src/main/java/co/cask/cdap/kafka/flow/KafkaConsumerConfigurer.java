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

/**
 * This class is for configuring Kafka consumer information for {@link KafkaConsumerFlowlet}.
 */
public interface KafkaConsumerConfigurer {

  /**
   * Default message fetch size in bytes when making Kafka fetch request.
   */
  int DEFAULT_FETCH_SIZE = 1048576;  // 1M

  /**
   * Adds a topic partition to consume message from. Same as calling
   *
   * {@link #addTopicPartition(String, int, int) addTopicPartition(topic, partition, DEFAULT_FETCH_SIZE)}
   *
   * @param topic name of the Kafka topic
   * @param partition partition number
   */
  void addTopicPartition(String topic, int partition);

  /**
   * Adds a topic partition to consume message from, using the given fetch size for each fetch request.
   *
   * @param topic name of the Kafka topic
   * @param partition partition number
   * @param fetchSize maximum number of bytes to fetch per request
   */
  void addTopicPartition(String topic, int partition, int fetchSize);

  /**
   * Sets an upper bound on the number of messages that will be processed before the offsets are committed.
   *
   * @param batchSize number of messages to process
   */
  void setProcessBatchSize(int batchSize);

  /**
   * Sets an approximation on the upper bound of time that will be spent processing messages before the offsets
   * are committed.
   *
   * @param maxProcessTimeMillis amount of time that can be spent processing events, in milliseconds
   */
  void setMaxProcessTime(long maxProcessTimeMillis);
}
