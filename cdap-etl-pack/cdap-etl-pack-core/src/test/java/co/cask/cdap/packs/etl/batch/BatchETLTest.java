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

package co.cask.cdap.packs.etl.batch;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.batch.sink.DictionarySink;
import co.cask.cdap.packs.etl.batch.source.TableSource;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.packs.etl.schema.Schema;
import co.cask.cdap.packs.etl.transform.schema.DefaultSchemaMapping;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.packs.etl.TestConstants.getInSchema;
import static co.cask.cdap.packs.etl.TestConstants.getMapping;
import static co.cask.cdap.packs.etl.TestConstants.getOutSchema;

/**
 *
 */
public class BatchETLTest extends TestBase {
  private static final Gson GSON = new Gson();

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
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, DictionarySink.class.getName());
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME, "users");
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "user_id");

    testApp(BatchETLApplicationConfiguredWithArgs.class, args);
  }

  @Test
  public void testAppConfiguredByMix() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // source configuration
    args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, "userDetails");
    Schema inSchema = setSourceSchema(args);

    // transformation configuration
    setTransformationSchemaAndMapping(args, inSchema);

    // sink configuration
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME, "users");
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "user_id");

    testApp(BatchETLConfiguredByMix.class, args);
  }

  @Test
  public void testAppConfiguredByCode() throws Exception {
    testApp(BatchETLConfiguredByCode.class, Collections.<String, String>emptyMap());
  }

  private void testApp(Class<? extends AbstractApplication> app, Map<String, String> args)
    throws Exception {

    // Simple ETL pipeline: MapReduce that takes input from table dataset and outputs into dictionary dataset using
    //                      identity function (no actual transform)

    ApplicationManager appMngr = deployApplication(app);

    DataSetManager<Table> table = getDataset("userDetails");
    table.get().put(new Put("fooKey").add("userId", "1").add("firstName", "alex").add("lastName", "baranau"));
    table.flush();

    MapReduceManager mr = appMngr.startMapReduce("ETLMapReduce", args);
    mr.waitForFinish(2, TimeUnit.MINUTES);

    // verify
    DataSetManager<DictionaryDataSet> dictionary = getDataset(Constants.DICTIONARY_DATASET);
    // NOTE: using int value
    Assert.assertEquals("alex", Bytes.toString(dictionary.get().get("users", Bytes.toBytes(1), "first_name")));
    Assert.assertEquals("baranau", Bytes.toString(dictionary.get().get("users", Bytes.toBytes(1), "last_name")));
  }

  private Schema setSourceSchema(Map<String, String> args) {
    Schema inSchema = getInSchema();
    args.put(Constants.Batch.Source.ARG_SOURCE_SCHEMA, GSON.toJson(inSchema));
    return inSchema;
  }

  private Schema setTransformationSchemaAndMapping(Map<String, String> args, Schema inSchema) {
    args.put(Constants.Transformation.Schema.ARG_INPUT_SCHEMA, GSON.toJson(inSchema));
    Schema outSchema = getOutSchema();
    args.put(Constants.Transformation.Schema.ARG_OUTPUT_SCHEMA, GSON.toJson(outSchema));
    args.put(Constants.Transformation.SchemaMapping.ARG_MAPPING, GSON.toJson(getMapping()));

    return outSchema;
  }
}
