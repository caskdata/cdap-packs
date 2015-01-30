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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.packs.etl.AbstractConfigurableProgram;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.Map;

/**
 * Outputs data into hbase table.
 */
public class HBaseSink extends AbstractConfigurableProgram<FlowletContext> implements RealtimeSink {

  private String zkQuorum = null;
  private Integer zkClientPort = null;
  private String zkParentNode = null;

  private String tableName = null;
  private String rowKeyField = null;
  private String family = null;

  //NOTE: these are set only at runtime

  // just to avoid redundant to byte[] conversions
  private byte[] familyBytes = null;
  private HTable hTable = null;

  public HBaseSink() {
    super();
  }

  public HBaseSink(String zkQuorum, Integer zkClientPort, String zkParentNode,
                   String tableName, String family, String rowKeyField) {
    this.zkQuorum = zkQuorum;
    this.zkClientPort = zkClientPort;
    this.zkParentNode = zkParentNode;
    this.tableName = tableName;
    this.rowKeyField = rowKeyField;
    this.family = family;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    putIfNotNull(args, Constants.Realtime.Sink.HBase.ARG_TABLE_NAME, tableName);
    putIfNotNull(args, Constants.Realtime.Sink.HBase.ARG_ROW_KEY_FIELD, rowKeyField);
    putIfNotNull(args, Constants.Realtime.Sink.HBase.ARG_COLFAM, family);
    putIfNotNull(args, Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM, zkQuorum);
    putIfNotNull(args, Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT, String.valueOf(zkClientPort));
    putIfNotNull(args, Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_PARENT_NODE, zkParentNode);

    return args;
  }

  private void putIfNotNull(Map<String, String> args, String key, String value) {
    if (value != null) {
      args.put(key, value);
    }
  }

  @Override
  public void initialize(FlowletContext context) throws IOException {
    zkQuorum = Programs.getRequiredArgOrProperty(context, Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM);
    zkClientPort =
      Integer.valueOf(Programs.getRequiredArgOrProperty(context, Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT));
    tableName = Programs.getRequiredArgOrProperty(context, Constants.Realtime.Sink.HBase.ARG_TABLE_NAME);
    family = Programs.getRequiredArgOrProperty(context, Constants.Realtime.Sink.HBase.ARG_COLFAM);
    familyBytes = Bytes.toBytes(family);
    rowKeyField = Programs.getRequiredArgOrProperty(context, Constants.Realtime.Sink.HBase.ARG_ROW_KEY_FIELD);
    zkParentNode = Programs.getRequiredArgOrProperty(context, Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_PARENT_NODE);

    Configuration conf = HBaseConfiguration.create();
    // https://github.com/apache/hbase/pull/10
    conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkParentNode);
    createTableIfNotExists(tableName, conf);
    hTable = new HTable(conf, tableName);
    // todo: allow configuring client-side buffer, etc.
  }

  private void createTableIfNotExists(String tableName, Configuration conf) throws IOException {
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    if (!hBaseAdmin.tableExists(tableName)) {
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
      tableDescriptor.addFamily(new HColumnDescriptor(family));
      hBaseAdmin.createTable(tableDescriptor);
    }
  }

  @Override
  public void write(Record record) throws Exception {
    byte[] key = record.getValue(rowKeyField);
    if (key == null) {
      // skipping this record
      return;
    }

    Put put = new Put(key);
    for (String fieldName : record.getFields()) {
      // we first get the typed value, then put it in bytes
      byte[] bytesValue = record.getValue(fieldName);
      put.add(familyBytes, Bytes.toBytes(fieldName), bytesValue);
    }

    if (put.isEmpty()) {
      // skipping this record
      return;
    }
    hTable.put(put);
  }

  @Override
  public void destroy() {
    super.destroy();
    Closeables.closeQuietly(hTable);
  }
}
