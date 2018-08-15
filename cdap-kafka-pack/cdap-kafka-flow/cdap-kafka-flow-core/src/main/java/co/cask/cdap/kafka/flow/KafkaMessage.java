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

import org.apache.twill.kafka.client.TopicPartition;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * Represents a Kafka message.
 *
 * @param <OFFSET> Type of message offset
 */
public class KafkaMessage<OFFSET> {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final TopicPartition topicPartition;
  private final OFFSET nextOffset;
  private final ByteBuffer key;
  private final ByteBuffer payload;

  public KafkaMessage(TopicPartition topicPartition, OFFSET nextOffset,
                      @Nullable ByteBuffer key, @Nullable ByteBuffer payload) {
    this.topicPartition = topicPartition;
    this.nextOffset = nextOffset;
    this.key = key == null ? EMPTY_BUFFER : key;
    this.payload = payload == null ? EMPTY_BUFFER : payload;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public OFFSET getNextOffset() {
    return nextOffset;
  }

  public ByteBuffer getKey() {
    return key;
  }

  public ByteBuffer getPayload() {
    return payload;
  }
}
