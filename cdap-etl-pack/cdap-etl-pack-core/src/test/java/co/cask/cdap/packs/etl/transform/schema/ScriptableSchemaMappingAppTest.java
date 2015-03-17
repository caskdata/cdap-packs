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

package co.cask.cdap.packs.etl.transform.schema;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScriptableSchemaMappingAppTest extends TestBase {

  @Test
  public void testConfigurationWithCode() throws Exception {
    testApp(RealtimeETLUsingScriptableMapping.class, Collections.<String, String>emptyMap());
  }

  protected void testApp(Class<? extends AbstractApplication> app, Map<String, String> args) throws Exception {

    ApplicationManager appMngr = deployApplication(app);

    // populate dictionary
    DataSetManager<DictionaryDataSet> dict = getDataset(Constants.DICTIONARY_DATASET);
    dict.get().write("users", Bytes.toBytes(55), ImmutableMap.of("firstName", Bytes.toBytes("jack")));
    dict.flush();

    StreamWriter sw = appMngr.getStreamWriter("usersStream");
    sw.send("55");

    FlowManager flow = appMngr.startFlow("ETLFlow", args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 5, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(5);

    // verify
    dict = getDataset(Constants.DICTIONARY_DATASET);
    Assert.assertEquals("jack", Bytes.toString(dict.get().get("myDict", Bytes.toBytes(55), "first_name")));

    flow.stop();

  }
}
