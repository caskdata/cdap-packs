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

package co.cask.cdap.packs.etl.batch.sink;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.batch.source.TableSource;
import co.cask.cdap.packs.etl.kafka.BaseKafkaTest;
import co.cask.cdap.packs.etl.schema.Schema;
import co.cask.cdap.packs.etl.transform.schema.DefaultSchemaMapping;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaSinkTest extends BaseKafkaTest {

  @Test
  public void testAppConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // source configuration
    args.put(Constants.Batch.Source.ARG_SOURCE_TYPE, TableSource.class.getName());
    args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, "userDetails1");
    Schema inSchema = setSourceSchema(args);

    // transformation configuration
    args.put(Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE, DefaultSchemaMapping.class.getName());
    setTransformationSchemaAndMapping(args, inSchema);

    // sink configuration
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, KafkaSink.class.getName());
    args.put(Constants.Batch.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM, zkConnectionStr);
    args.put(Constants.Batch.Sink.Kafka.ARG_KAFKA_TOPIC, "topic1");

    testApp(BatchETLToKafkaConfiguredWithArgs.class, args, "topic1", "userDetails1");
  }

  @Test
  public void testConfigurationWithCode() throws Exception {
    testApp(BatchETLToKafkaConfiguredWithCode.class, Collections.<String, String>emptyMap(), "topic2", "userDetails2");
  }

  private void testApp(Class<? extends AbstractApplication> app, Map<String, String> args, String topic, String tableName)
    throws Exception {
    ApplicationManager appMngr = deployApplication(app);

    DataSetManager<Table> table = getDataset(tableName);
    table.get().put(new Put("fooKey").add("userId", "55").add("firstName", "jack").add("lastName", "brown"));
    table.get().put(new Put("barKey").add("userId", "49").add("firstName", "jim").add("lastName", "smith"));
    table.get().put(new Put("bazKey").add("userId", "300").add("firstName", "alex").add("lastName", "roberts"));
    table.flush();

    MapReduceManager mr = appMngr.startMapReduce("ETLMapReduce", args);
    mr.waitForFinish(2, TimeUnit.MINUTES);

    Map<Integer, Map<String, byte[]>> expected = ImmutableMap.of(
      55, mapOf(55, "jack", "brown"),
      49, mapOf(49, "jim", "smith"),
      300, mapOf(300, "alex", "roberts")
    );
    verifyKafkaResults(topic, expected);
  }
}
