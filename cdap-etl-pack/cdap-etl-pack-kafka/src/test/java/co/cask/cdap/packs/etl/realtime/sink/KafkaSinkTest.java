/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.packs.etl.realtime.sink;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.batch.source.TableSource;
import co.cask.cdap.packs.etl.kafka.BaseKafkaTest;
import co.cask.cdap.packs.etl.schema.Schema;
import co.cask.cdap.packs.etl.transform.schema.DefaultSchemaMapping;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class KafkaSinkTest extends BaseKafkaTest {

  @Test
  public void testAppConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // source configuration
    args.put(Constants.Batch.Source.ARG_SOURCE_TYPE, TableSource.class.getName());
    args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, "userDetails");
    Schema inSchema = setSourceSchema(args);

    // transformation configuration
    args.put(Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE, DefaultSchemaMapping.class.getName());
    setTransformationSchemaAndMapping(args, inSchema);

    // sink configuration
    // todo change to Realtime.Sink?
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, co.cask.cdap.packs.etl.batch.sink.KafkaSink.class.getName());
    args.put(Constants.Realtime.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM, zkConnectionStr);
    args.put(Constants.Realtime.Sink.Kafka.ARG_KAFKA_TOPIC, "topic1");

    testApp(RealtimeETLToKafkaConfiguredWithArgs.class, args, "topic1", "stream1");
  }

  @Test
  public void testConfigurationWithCode() throws Exception {
    testApp(RealtimeETLToKafkaConfiguredWithCode.class, Collections.<String, String>emptyMap(), "topic2", "stream2");
  }

  private void testApp(Class<? extends AbstractApplication> app, Map<String, String> args, String topic, String stream)
    throws TimeoutException, InterruptedException, IOException {

    ApplicationManager appMngr = deployApplication(app);

    StreamManager sw = getStreamManager(stream);
    sw.send("55,jack,brown");
    sw.send("49,jim,smith");
    sw.send("300,alex,roberts");

    FlowManager flow = appMngr.getFlowManager("ETLFlow").start(args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(2, 10, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);
    flow.stop();

    Map<Integer, Map<String, byte[]>> expected = ImmutableMap.of(
      55, mapOf(55, "jack", "brown"),
      49, mapOf(49, "jim", "smith"),
      300, mapOf(300, "alex", "roberts")
    );
    verifyKafkaResults(topic, expected);
  }
}
