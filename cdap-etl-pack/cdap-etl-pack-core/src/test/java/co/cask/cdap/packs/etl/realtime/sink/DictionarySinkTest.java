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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
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
  public void testConfigurationWithCode() throws Exception {
    testApp(RealtimeETLToDictionaryConfiguredWithCode.class, Collections.<String, String>emptyMap());
  }

  @Test
  public void testConfigurationWithArgs() throws Exception {
    ImmutableMap<String, String> args =
        ImmutableMap.of(Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_NAME, "myDict",
                        Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "fname");
    testApp(RealtimeETLToDictionaryConfiguredWithArgs.class, args);
  }

  protected void testApp(Class<? extends AbstractApplication> app, Map<String, String> args) throws Exception {

    ApplicationManager appMngr = deployApplication(app);

    StreamManager sw = getStreamManager("filesStream");
    sw.send(ImmutableMap.of("fname", "file1", "size", "1024"), "ignored_body");

    FlowManager flow = appMngr.getFlowManager("ETLFlow").start(args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 5, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);

    // verify
    DataSetManager<DictionaryDataSet> dict = getDataset(Constants.DICTIONARY_DATASET);
    Assert.assertEquals("1024", Bytes.toString(dict.get().get("myDict", "file1".getBytes(Charsets.UTF_8), "size")));
    Assert.assertEquals("file1", Bytes.toString(dict.get().get("myDict", "file1".getBytes(Charsets.UTF_8), "fname")));

    flow.stop();

  }
}
