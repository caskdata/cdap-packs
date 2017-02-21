/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FailurePolicy;
import co.cask.cdap.api.flow.flowlet.FailureReason;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.InputContext;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * Abstract base class for implementing consuming data from a Kafka cluster. This class serves as the generic base
 * to help in implementing a flowlet that can poll from a specific Kafka version. Users should be extending from one
 * of the sub-classes of this class.
 *
 * @param <KEY> Type of message key
 * @param <PAYLOAD> Type of message value
 * @param <OFFSET> Type of offset object
 */
public abstract class KafkaConsumerFlowlet<KEY, PAYLOAD, OFFSET> extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerFlowlet.class);
  protected static final int SO_TIMEOUT = 5 * 1000;           // 5 seconds.

  private final Function<KafkaConsumerInfo<OFFSET>, OFFSET> consumerToOffset =
    new Function<KafkaConsumerInfo<OFFSET>, OFFSET>() {
    @Override
    public OFFSET apply(KafkaConsumerInfo<OFFSET> input) {
      return input.getReadOffset();
    }
  };

  private Function<ByteBuffer, KEY> keyDecoder;
  private Function<ByteBuffer, PAYLOAD> payloadDecoder;
  private KafkaConfig kafkaConfig;
  private Map<TopicPartition, KafkaConsumerInfo<OFFSET>> consumerInfos;
  private Map<TopicPartition, KafkaConsumerInfo<OFFSET>> changedConsumerInfos;
  private int instances;

  private DefaultKafkaConfigurer kafkaConfigurer;
  private int maxProcessEvents;
  private long maxProcessMillis;

  /**
   * Initialize this {@link Flowlet}. Child class must call this method explicitly when overriding it.
   */
  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);

    instances = context.getInstanceCount();

    // Tries to detect key and payload decoder based on the class parameterized type.
    Type superType = TypeToken.of(getClass()).getSupertype(KafkaConsumerFlowlet.class).getType();

    // Tries to detect Key and Payload type for creating decoder for them
    if (superType instanceof ParameterizedType) {
      // Extract Key and Payload types
      Type[] typeArgs = ((ParameterizedType) superType).getActualTypeArguments();

      // Parameter type arguments of AbstractKafkaConsumerFlowlet must be 2
      keyDecoder = createKeyDecoder(typeArgs[0]);
      payloadDecoder = createPayloadDecoder(typeArgs[1]);
    }

    // Configure kafka
    DefaultKafkaConfigurer kafkaConfigurer = new DefaultKafkaConfigurer();
    configureKafka(kafkaConfigurer);

    if (kafkaConfigurer.getZookeeper() == null && kafkaConfigurer.getBrokers() == null) {
      throw new IllegalStateException("Kafka not configured. Must provide either zookeeper or broker list.");
    }

    kafkaConfig = new KafkaConfig(kafkaConfigurer.getZookeeper(), kafkaConfigurer.getBrokers());
    consumerInfos = createConsumerInfos(kafkaConfigurer.getTopicPartitions());
    changedConsumerInfos = consumerInfos;

    this.kafkaConfigurer = kafkaConfigurer;
    updateMaxProcessLimits();
  }

  /**
   * A {@link Tick} method that triggered periodically by the Flow system to poll messages from Kafka.
   * The default poll delay is 100 milliseconds. This method can be overridden to provide different delay value.
   * <br/>
   * E.g. to override with 1 second instead:
   *
   * <pre>
   * {@literal @}Override
   * {@literal @}Tick(delay = 1, unit = TimeUnit.SECONDS)
   * public void pollMessages() {
   *   super.pollMessages();
   * }
   * </pre>
   */
  @Tick(delay = 100, unit = TimeUnit.MILLISECONDS)
  public void pollMessages() throws Exception {

    // Detect and handle instance count change
    if (instances != getContext().getInstanceCount()) {
      DefaultKafkaConfigurer kafkaConfigurer = new DefaultKafkaConfigurer();
      handleInstancesChanged(kafkaConfigurer);
      changedConsumerInfos = Maps.newHashMap(consumerInfos);
      updateConsumerInfos(kafkaConfigurer.getTopicPartitions(), changedConsumerInfos);
      return;
    }

    boolean infosUpdated = false;
    // Poll for messages from Kafka
    for (KafkaConsumerInfo<OFFSET> info : consumerInfos.values()) {
      Stopwatch stopwatch = new Stopwatch().start();
      int numProcessed = 0;
      Iterator<KafkaMessage<OFFSET>> iterator = readMessages(info);
      while (iterator.hasNext() && numProcessed++ < maxProcessEvents && stopwatch.elapsedMillis() < maxProcessMillis) {
        KafkaMessage<OFFSET> message = iterator.next();
        processMessage(message);

        // Update the read offset
        info.setReadOffset(message.getNextOffset());
      }
      if (info.hasPendingChanges()) {
        infosUpdated = true;
      }
    }

    // Save new offset if there is at least one message processed, or even if the offset simply changed.
    if (infosUpdated) {
      saveReadOffsets(Maps.transformValues(consumerInfos, consumerToOffset));
    }
  }

  @Override
  public void onSuccess(Object input, InputContext inputContext) {
    super.onSuccess(input, inputContext);

    // Input object is null for @Tick method
    if (input != null) {
      return;
    }

    for (KafkaConsumerInfo<OFFSET> info : consumerInfos.values()) {
      info.commitReadOffset();
    }

    if (getContext().getInstanceCount() != instances) {
      instances = getContext().getInstanceCount();
      consumerInfos = ImmutableMap.copyOf(changedConsumerInfos);
    }
  }

  @Override
  public FailurePolicy onFailure(Object input, InputContext inputContext, FailureReason reason) {
    if (input == null) {
      for (KafkaConsumerInfo<OFFSET> info : consumerInfos.values()) {
        info.rollbackReadOffset();
      }
    }
    return FailurePolicy.RETRY;
  }

  /**
   * Override to return a {@link KeyValueTable} for storing consumer offsets.
   */
  protected KeyValueTable getOffsetStore() {
    return null;
  }


  /**
   * Configure Kafka consumer. This method will be called during the {@link #initialize(FlowletContext)} phase,
   * hence it has access to {@link FlowletContext} through the {@link #getContext()} method.
   *
   * @param configurer for configuring consuming from Kafka
   */
  protected abstract void configureKafka(KafkaConfigurer configurer);

  /**
   * Read messages from Kafka.
   *
   * @param consumerInfo Contains information about where to fetch messages from
   * @return An {@link Iterator} containing sequence of messages read from Kafka. The first message must
   *         has offset no earlier than the {@link KafkaConsumerInfo#getReadOffset()} as given in the parameter.
   */
  protected abstract Iterator<KafkaMessage<OFFSET>> readMessages(KafkaConsumerInfo<OFFSET> consumerInfo);

  /**
   * Returns the read offsets to start with for the given {@link TopicPartition}.
   */
  protected abstract OFFSET getBeginOffset(TopicPartition topicPartition);

  /**
   * Persists read offsets for all topic-partition that this Flowlet consumes from Kafka.
   */
  protected abstract void saveReadOffsets(Map<TopicPartition, OFFSET> offsets);

  /**
   * Override to handle changes in flowlet instances. Sub-class may do rebalancing of topic partition that it consumes.
   *
   * @param configurer for configuring consuming from Kafka
   */
  protected void handleInstancesChanged(KafkaConsumerConfigurer configurer) {
    // No-op
  }

  /**
   * Returns a Kafka configuration.
   */
  protected final KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }

  /**
   * Overrides this method if interested in the raw Kafka message.
   *
   * @param message The message fetched from Kafka.
   */
  protected void processMessage(KafkaMessage<OFFSET> message) throws Exception {
    processMessage(decodeKey(message.getKey()), decodePayload(message.getPayload()));
  }

  /**
   * Override this method if interested in both the key and payload of a message read from Kafka.
   *
   * @param key Key decoded from the message
   * @param payload Payload decoded from the message
   */
  protected void processMessage(KEY key, PAYLOAD payload) throws Exception {
    processMessage(payload);
  }

  /**
   * Override this method if only interested in the payload of a message read from Kafka.
   *
   * @param payload Payload decoded from the message
   */
  protected void processMessage(PAYLOAD payload) throws Exception {
    // No-op by default.
  }

  /**
   * Override this method to provide custom decoding of a message key.
   *
   * @param buffer The bytes representing the key in the Kafka message
   * @return The decoded key
   */
  protected KEY decodeKey(ByteBuffer buffer) {
    return (keyDecoder != null) ? keyDecoder.apply(buffer) : null;
  }

  /**
   * Override this method to provide custom decoding of a message payload.
   *
   * @param buffer The bytes representing the payload in the Kafka message
   * @return The decoded payload
   */
  protected PAYLOAD decodePayload(ByteBuffer buffer) {
    return (payloadDecoder != null) ? payloadDecoder.apply(buffer) : null;
  }

  /**
   * Stops a {@link Service} and waits for the completion. If there is exception during stop, it will get logged.
   */
  protected final void stopService(Service service) {
    try {
      service.stopAndWait();
    } catch (Throwable t) {
      LOG.error("Failed when stopping service {}", service, t);
    }
  }

  /**
   * Returns the key to be used when persisting offsets into a {@link KeyValueTable}.
   */
  protected String getStoreKey(TopicPartition topicPartition) {
    return topicPartition.getTopic() + ":" + topicPartition.getPartition();
  }

  /**
   * Creates a decoder for the key type.
   *
   * @param type type to decode to
   */
  private Function<ByteBuffer, KEY> createKeyDecoder(Type type) {
    return createDecoder(type, "No decoder for decoding message key");
  }

  /**
   * Creates a decoder for the payload type.
   *
   * @param type type to decode to
   */
  private Function<ByteBuffer, PAYLOAD> createPayloadDecoder(Type type) {
    return createDecoder(type, "No decoder for decoding message payload");
  }

  /**
   * Creates a decoder for decoding {@link ByteBuffer} for known type. It supports
   * <p/>
   * <pre>
   * - String (assuming UTF-8)
   * - byte[]
   * - ByteBuffer
   * </pre>
   *
   * @param type type to decode to
   * @param failureDecodeMessage message for the exception if decoding of the given type is not supported
   * @param <T> Type of the decoded type
   * @return A {@link Function} that decode {@link ByteBuffer} into the given type or a failure decoder created through
   *         {@link #createFailureDecoder(String)} if the type is not support
   */
  @SuppressWarnings("unchecked")
  private <T> Function<ByteBuffer, T> createDecoder(Type type, String failureDecodeMessage) {
    if (String.class.equals(type)) {
      return (Function<ByteBuffer, T>) createStringDecoder();
    }
    if (ByteBuffer.class.equals(type)) {
      return (Function<ByteBuffer, T>) createByteBufferDecoder();
    }
    if (byte[].class.equals(type) || (type instanceof GenericArrayType &&
      byte.class.equals(((GenericArrayType) type).getGenericComponentType()))) {
      return (Function<ByteBuffer, T>) createBytesDecoder();
    }
    return createFailureDecoder(failureDecodeMessage);
  }

  /**
   * Creates a decoder that convert the input {@link ByteBuffer} into UTF-8 String. The input {@link ByteBuffer}
   * will not be consumed after the call.
   */
  private Function<ByteBuffer, String> createStringDecoder() {
    return new Function<ByteBuffer, String>() {
      @Override
      public String apply(ByteBuffer input) {
        input.mark();
        String result = Charsets.UTF_8.decode(input).toString();
        input.reset();
        return result;
      }
    };
  }

  /**
   * Creates a decoder that returns the same input {@link ByteBuffer}.
   */
  private Function<ByteBuffer, ByteBuffer> createByteBufferDecoder() {
    return new Function<ByteBuffer, ByteBuffer>() {
      @Override
      public ByteBuffer apply(ByteBuffer input) {
        return input;
      }
    };
  }

  /**
   * Creates a decoder that reads {@link ByteBuffer} content and return it as {@code byte[]}. The
   * input {@link ByteBuffer} will not be consumed after the call.
   */
  private Function<ByteBuffer, byte[]> createBytesDecoder() {
    return new Function<ByteBuffer, byte[]>() {
      @Override
      public byte[] apply(ByteBuffer input) {
        byte[] bytes = new byte[input.remaining()];
        input.mark();
        input.get(bytes);
        input.reset();
        return bytes;
      }
    };
  }

  /**
   * Creates a decoder that always decode fail by raising an {@link IllegalStateException}.
   */
  private <T> Function<ByteBuffer, T> createFailureDecoder(final String failureMessage) {
    return new Function<ByteBuffer, T>() {
      @Override
      public T apply(ByteBuffer input) {
        throw new IllegalStateException(failureMessage);
      }
    };
  }

  private Map<TopicPartition, KafkaConsumerInfo<OFFSET>> createConsumerInfos(Map<TopicPartition, Integer> config) {
    ImmutableMap.Builder<TopicPartition, KafkaConsumerInfo<OFFSET>> consumers = ImmutableMap.builder();

    for (Map.Entry<TopicPartition, Integer> entry : config.entrySet()) {
      consumers.put(entry.getKey(),
                    new KafkaConsumerInfo<>(entry.getKey(), entry.getValue(), getBeginOffset(entry.getKey())));
    }
    return consumers.build();
  }

  private void updateConsumerInfos(final Map<TopicPartition, Integer> config,
                                   Map<TopicPartition, KafkaConsumerInfo<OFFSET>> consumerInfos) {
    // Remove consumer infos that are no longer needed.
    Iterables.removeIf(consumerInfos.entrySet(), new Predicate<Map.Entry<TopicPartition, KafkaConsumerInfo<OFFSET>>>() {
      @Override
      public boolean apply(Map.Entry<TopicPartition, KafkaConsumerInfo<OFFSET>> input) {
        return !config.containsKey(input.getKey());
      }
    });

    // Add new topic partition
    for (Map.Entry<TopicPartition, Integer> entry : config.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      int fetchSize = entry.getValue();

      KafkaConsumerInfo<OFFSET> info = consumerInfos.get(topicPartition);
      if (info != null) {
        // If the consumer info already exists, just update the fetch size if it changed
        if (info.getFetchSize() != fetchSize) {
          consumerInfos.put(topicPartition,
                            new KafkaConsumerInfo<>(topicPartition, fetchSize, info.getReadOffset()));
        }
      } else {
        // Otherwise, create a new consumer info
        consumerInfos.put(topicPartition,
                          new KafkaConsumerInfo<>(topicPartition, fetchSize,
                                                  getBeginOffset(entry.getKey())));
      }
    }
    updateMaxProcessLimits();
  }

  // we need to update the limits whenever the number of consumerInfos may have changed
  private void updateMaxProcessLimits() {
    // split the limits across the different consumerInfos. Otherwise, the first few consumerInfos may
    // starve the later consumerInfos
    maxProcessMillis = kafkaConfigurer.getMaxProcessTimeMillis() / consumerInfos.values().size();
    // minimum batch size of 1 (for instance, user might configure batch size of 10, and there may be 8 topic
    // partitions). This would otherwise result in a batch size of 0
    maxProcessEvents = Math.max(1, kafkaConfigurer.getBatchSize() / consumerInfos.values().size());
  }
}
