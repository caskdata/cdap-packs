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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DictionarySinkTest extends TestBase {

  @Test
  public void testConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // sink configuration
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, DictionarySink.class.getName());
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME, "users1");
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "user_id");

    // NOTE: using unique table and dic name to avoid clash with other tests
    testApp(BatchETLToDictionaryConfiguredWithArgs.class, args, "userDetails1", "users1");
  }

  @Test
  public void testConfiguredByCode() throws Exception {
    // NOTE: using unique table and dic name to avoid clash with other tests
    testApp(BatchETLToDictionaryConfiguredWithCode.class, Collections.<String, String>emptyMap(), "userDetails2", "users2");
  }

  private void testApp(Class<? extends AbstractApplication> app,
                       Map<String, String> args, String tableName, String dictionaryName)
    throws Exception {

    // simple etl: mr job takes input from table dataset and outputs into dictionary dataset using
    //                      identity function (no actual transform)

    ApplicationManager appMngr = deployApplication(app);

    DataSetManager<Table> table = getDataset(tableName);
    table.get().put(new Put("fooKey").add("userId", "23").add("firstName", "jack").add("lastName", "brown"));
    table.flush();

    MapReduceManager mr = appMngr.startMapReduce("ETLMapReduce", args);
    mr.waitForFinish(2, TimeUnit.MINUTES);

    // verify
    DataSetManager<DictionaryDataSet> dictionary = getDataset(Constants.DICTIONARY_DATASET);
    // NOTE: using int value
    Assert.assertEquals(23, Bytes.toInt(dictionary.get().get(dictionaryName, Bytes.toBytes(23), "user_id")));
    Assert.assertEquals("jack", Bytes.toString(dictionary.get().get(dictionaryName, Bytes.toBytes(23), "first_name")));
    Assert.assertEquals("brown", Bytes.toString(dictionary.get().get(dictionaryName, Bytes.toBytes(23), "last_name")));
  }
}
