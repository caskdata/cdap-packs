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
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.hbase.HBase96Test;
import co.cask.cdap.packs.etl.hbase.HBaseTestBase;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HBaseSinkTest extends TestBase {
  private static final String CF = "family";
  private static final String TABLE = "table";
  private static String zkConnectionAddress;
  private static int zkClientPort;
  private static HBaseTestBase hBaseTestBase;

  @BeforeClass
  public static void setupETLHBaseSinkTest() throws Exception {
    hBaseTestBase = new HBase96Test();
    hBaseTestBase.startHBase();
    zkConnectionAddress = hBaseTestBase.getZkConnectionString();
    zkClientPort = hBaseTestBase.zkCluster.getClientPort();
  }

  @AfterClass
  public static void cleanupETLHBaseSinkTest() throws Exception {
    hBaseTestBase.stopHBase();
  }

  @Test
  public void testConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // sink configuration
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, HBaseSink.class.getName());
    args.put(Constants.Batch.Sink.HBase.ARG_ROW_KEY_FIELD, "user_id");
    args.put(Constants.Batch.Sink.HBase.ARG_TABLE_NAME, TABLE);
    args.put(Constants.Batch.Sink.HBase.ARG_TABLE_FAMILY, CF);
    args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT, String.valueOf(zkClientPort));
    args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM, zkConnectionAddress.split(":")[0]);
    args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_PARENT_NODE, "/hbase");

    testApp(BatchETLToHBaseConfiguredWithArgs.class, args);
  }

  // todo: test configured by code

  private void testApp(Class<? extends AbstractApplication> app, Map<String, String> args) throws Exception {

    // ETL: mr job takes input from table dataset and outputs into HBase table using simple schema mapping transform

    ApplicationManager appMngr = deployApplication(app);

    DataSetManager<Table> table = getDataset("userDetails3");
    table.get().put(new Put("fooKey").add("userId", "55").add("firstName", "jack").add("lastName", "brown"));
    table.flush();

    MapReduceManager mr = appMngr.getMapReduceManager("ETLMapReduce").start(args);
    mr.waitForFinish(2, TimeUnit.MINUTES);

    // verify
    HTable hTable = hBaseTestBase.getHTable(Bytes.toBytes(TABLE));
    byte[] row = Bytes.toBytes(55);
    Get get = new Get(row);
    Result result = hTable.get(get);

    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(55, Bytes.toInt(result.getRow()));
    Assert.assertEquals(55, Bytes.toInt(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("user_id"))));
    Assert.assertEquals("jack", Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("first_name"))));
    Assert.assertEquals("brown", Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("last_name"))));
  }
}
