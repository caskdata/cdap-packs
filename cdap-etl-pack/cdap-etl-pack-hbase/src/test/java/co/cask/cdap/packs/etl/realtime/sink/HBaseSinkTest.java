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
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.hbase.HBase96Test;
import co.cask.cdap.packs.etl.hbase.HBaseTestBase;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.packs.etl.TestConstants.CF;
import static co.cask.cdap.packs.etl.TestConstants.getOutSchema;

/**
 *
 */
public class HBaseSinkTest extends TestBase {
  private static String zkConnectionAddress;
  private static int zkClientPort;
  private static HBaseTestBase hBaseTestBase;

  @BeforeClass
  public static void setupETLHBaseSinkTest() throws Exception {
    hBaseTestBase = new HBase96Test();
    hBaseTestBase.startHBase();
    zkConnectionAddress = hBaseTestBase.getZkConnectionString();
    System.setProperty("zk.host", zkConnectionAddress);
    zkClientPort = hBaseTestBase.zkCluster.getClientPort();
    System.setProperty("zk.port", String.valueOf(zkClientPort));
  }

  @AfterClass
  public static void cleanupETLHBaseSinkTest() throws Exception {
    hBaseTestBase.stopHBase();
  }

  @Test @Ignore
  public void testConfigurationWithArgs() throws Exception {
    // sink configuration
    ImmutableMap<String, String> args = ImmutableMap.<String, String>builder()
      .put(SchemaSink.ARG_SCHEMA, new Gson().toJson(getOutSchema()))
      .put(Constants.Realtime.Sink.HBase.ARG_ROW_KEY_FIELD, "user_id")
      .put(Constants.Realtime.Sink.HBase.ARG_TABLE_NAME, "table1")
      .put(Constants.Realtime.Sink.HBase.ARG_COLFAM, CF)
      .put(Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM, zkConnectionAddress.split(":")[0])
      .put(Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT, String.valueOf(zkClientPort))
      .put(Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_PARENT_NODE, String.valueOf("/hbase")).build();
    testApp(RealtimeETLToHBaseConfiguredWithArgs.class, args, "stream1", "table1");
  }

  @Test @Ignore
  public void testConfigurationWithCode() throws Exception {
    testApp(RealtimeETLToHBaseConfiguredWithCode.class, Collections.<String, String>emptyMap(), "stream2", "table2");
  }

  protected void testApp(Class<? extends AbstractApplication> app, Map<String, String> args,
                         String streamName, String tableName) throws Exception {

    ApplicationManager appMngr = deployApplication(app);

    StreamWriter sw = appMngr.getStreamWriter(streamName);
    sw.send("55,jack,brown");

    FlowManager flow = appMngr.startFlow("ETLFlow", args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 20, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);

    // verify
    HTable hTable = hBaseTestBase.getHTable(Bytes.toBytes(tableName));
    byte[] row = Bytes.toBytes(55);
    Get get = new Get(row);
    Result result = hTable.get(get);

    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(55, Bytes.toInt(result.getRow()));
    Assert.assertEquals(55, Bytes.toInt(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("user_id"))));
    Assert.assertEquals("jack", Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("first_name"))));
    Assert.assertEquals("brown", Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("last_name"))));

    flow.stop();

  }
}
