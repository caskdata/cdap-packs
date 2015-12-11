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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class KafkaConsumingApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("KafkaConsumingApp");
    addFlow(new KafkaConsumingFlow(getKafkaSourceFlowlet()));
    createDataset("kafkaOffsets", KeyValueTable.class);
    createDataset("counter", KeyValueTable.class);
  }


  public static final class KafkaConsumingFlow extends AbstractFlow {

    private final Flowlet sourceFlowlet;

    KafkaConsumingFlow(Flowlet sourceFlowlet) {
      this.sourceFlowlet = sourceFlowlet;
    }

    @Override
    public void configure() {
      setName("KafkaConsumingFlow");
      addFlowlet(sourceFlowlet);
      addFlowlet(new DataSink());
      connect(sourceFlowlet, new DataSink());
    }
  }

  public static final class DataSink extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(DataSink.class);

    @UseDataSet("counter")
    private KeyValueTable counter;

    @ProcessInput
    public void process(String string) {
      LOG.info("Received: {}", string);
      counter.increment(Bytes.toBytes(string), 1L);
    }
  }

  /**
   * Returns the {@link Flowlet} instance for consuming data from Kafka.
   */
  protected abstract Flowlet getKafkaSourceFlowlet();
}
