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

package co.cask.cdap.packs.etl.realtime;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.TestConstants;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.packs.etl.realtime.sink.DictionarySink;
import co.cask.cdap.packs.etl.realtime.source.SchemaSource;
import co.cask.cdap.packs.etl.schema.Schema;
import co.cask.cdap.packs.etl.transform.schema.DefaultSchemaMapping;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Testing {@link RealtimeETL} pipeline as a whole
 */
public class RealtimeETLTest extends TestBase {
  private static final Gson GSON = new Gson();
  protected static final String USER = "1";
  protected static final String FIRST_NAME = "alex";
  protected static final String LAST_NAME = "baranau";

  @Test
  public void testAppConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // source configuration
    args.put(Constants.Realtime.Source.ARG_SOURCE_TYPE, SchemaSource.class.getName());
    args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, "userDetails");
    args.put(Constants.Realtime.Transformation.ARG_TRANSFORMATION_TYPE, DefaultSchemaMapping.class.getName());
    Schema inSchema = setSourceSchema(args);

    // transformation configuration
    setTransformationSchemaAndMapping(args, inSchema);

    // sink configuration
    args.put(Constants.Realtime.Sink.ARG_SINK_TYPE, DictionarySink.class.getName());
    args.put(Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_NAME, "users");
    args.put(Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "user_id");

    testApp(RealtimeETLConfigureByArgs.class, args);
  }

  @Test
  public void testAppConfiguredByCode() throws Exception {
    testApp(RealtimeETLConfigureByCode.class, Collections.<String, String>emptyMap());
  }

  protected void testApp(Class<? extends AbstractApplication> app, Map<String, String> args) throws Exception {
    // Simple ETL pipeline: data from the stream transformed with simple filed mapping logic and written to dictionary

    ApplicationManager appMngr = deployApplication(app);

    StreamManager sw = getStreamManager("userDetailsStream");
    sw.send(USER + "," + FIRST_NAME + "," + LAST_NAME);

    FlowManager flow = appMngr.getFlowManager("ETLFlow").start(args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 5, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);

    // verify
    DataSetManager<DictionaryDataSet> dictionary = getDataset(Constants.DICTIONARY_DATASET);
    // NOTE: using int value
    Assert.assertEquals(FIRST_NAME, Bytes.toString(dictionary.get().get("users", Bytes.toBytes(1), "first_name")));
    Assert.assertEquals(LAST_NAME, Bytes.toString(dictionary.get().get("users", Bytes.toBytes(1), "last_name")));

    flow.stop();
  }

  private Schema setSourceSchema(Map<String, String> args) {
    Schema inSchema = TestConstants.getInSchema();
    args.put(Constants.Realtime.Source.ARG_SOURCE_SCHEMA, GSON.toJson(inSchema));
    return inSchema;
  }

  private Schema setTransformationSchemaAndMapping(Map<String, String> args, Schema inSchema) {
    args.put(Constants.Transformation.Schema.ARG_INPUT_SCHEMA, GSON.toJson(inSchema));
    Schema outSchema = TestConstants.getOutSchema();
    args.put(Constants.Transformation.Schema.ARG_OUTPUT_SCHEMA, GSON.toJson(outSchema));
    args.put(Constants.Transformation.SchemaMapping.ARG_MAPPING, GSON.toJson(TestConstants.getMapping()));

    return outSchema;
  }
}
