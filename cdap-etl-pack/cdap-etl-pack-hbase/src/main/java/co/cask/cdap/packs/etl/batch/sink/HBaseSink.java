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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.packs.etl.AbstractConfigurableProgram;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * Outputs data into hbase table.
 */
public class HBaseSink extends AbstractConfigurableProgram<MapReduceContext> implements MapReduceSink {

  private String tableName = null;
  private String rowKeyField = null;
  private String family = null;
  private String zkQuorum = null;
  private String zkParentNode = null;
  private Integer zkClientPort = null;
  private byte[] familyBytes = null;

  public HBaseSink() {
    super();
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (tableName != null) {
      args.put(Constants.Batch.Sink.HBase.ARG_TABLE_NAME, tableName);
    }
    if (rowKeyField != null) {
      args.put(Constants.Batch.Sink.HBase.ARG_ROW_KEY_FIELD, rowKeyField);
    }
    if (family != null) {
      args.put(Constants.Batch.Sink.HBase.ARG_TABLE_FAMILY, family);
    }
    if (zkQuorum != null) {
      args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM, zkQuorum);
    }
    if (zkClientPort != null) {
      args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT, String.valueOf(zkClientPort));
    }
    if (zkParentNode != null) {
      args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_PARENT_NODE, String.valueOf(zkParentNode));
    }

    return args;
  }

  @Override
  public void initialize(MapReduceContext context) throws IOException {
    setFields(context);
  }

  @Override
  public void prepareJob(MapReduceContext context) throws IOException {
    setFields(context);
    Job job = context.getHadoopJob();
    Configuration mapredConf = job.getConfiguration();
    mapredConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    mapredConf.set(TableOutputFormat.QUORUM_ADDRESS, zkQuorum + ":" + zkClientPort + ":" + zkParentNode);
    mapredConf.setInt(TableOutputFormat.QUORUM_PORT, zkClientPort);

    job.setOutputFormatClass(TableOutputFormat.class);
    job.setOutputKeyClass(Integer.class);
    job.setOutputValueClass(Put.class);

    createTableIfNotExists(tableName, zkQuorum, zkClientPort);
  }

  private void setFields(MapReduceContext context) throws IOException {
    rowKeyField = Programs.getArgOrProperty(context, Constants.Batch.Sink.HBase.ARG_ROW_KEY_FIELD);
    Preconditions.checkArgument(rowKeyField != null, "Missing required argument " + Constants.Batch.Sink.HBase.ARG_ROW_KEY_FIELD);
    family = Programs.getArgOrProperty(context, Constants.Batch.Sink.HBase.ARG_TABLE_FAMILY);
    Preconditions.checkArgument(family != null, "Missing required argument " + Constants.Batch.Sink.HBase.ARG_TABLE_FAMILY);
    tableName = Programs.getArgOrProperty(context, Constants.Batch.Sink.HBase.ARG_TABLE_NAME);
    Preconditions.checkArgument(tableName != null, "Missing required argument " + Constants.Batch.Sink.HBase.ARG_TABLE_NAME);
    zkQuorum = Programs.getArgOrProperty(context, Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM);
    Preconditions.checkArgument(zkQuorum != null,
                                "Missing required argument " + Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM);
    zkClientPort = Integer.valueOf(Programs.getArgOrProperty(context, Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT));
    Preconditions.checkArgument(zkClientPort != null,
                                "Missing required argument " + Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT);
    zkParentNode = Programs.getArgOrProperty(context, Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_PARENT_NODE);
    familyBytes = Bytes.toBytes(family);
    zkParentNode = zkParentNode == null ? HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT : zkParentNode;
  }

  private void createTableIfNotExists(String tableName, String zkQuorum, int zkClientPort) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // https://github.com/apache/hbase/pull/10
    conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkParentNode);
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    if (!hBaseAdmin.tableExists(tableName)) {
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
      tableDescriptor.addFamily(new HColumnDescriptor(family));
      hBaseAdmin.createTable(tableDescriptor);
    }
  }

  @Override
  public void write(Mapper.Context context, Record record) throws IOException, InterruptedException {
    byte[] key = record.getValue(rowKeyField);
    if (key == null) {
      // skipping this record
      return;
    }

    Put put = new Put(key);
    for (String fieldName : record.getFields()) {
      byte[] bytesValue = record.getValue(fieldName);
      put.add(familyBytes, Bytes.toBytes(fieldName), bytesValue);
    }
    if (put.isEmpty()) {
      // skipping this record
      return;
    }
    context.write(0, put);
  }
}
