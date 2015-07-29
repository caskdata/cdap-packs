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

package co.cask.cdap.packs.etl;

/**
 * ETL framework cosntants
 */
public final class Constants {
  // todo: shorten constants names

  /** Name of the dictionary dataset */
  public static final String DICTIONARY_DATASET = "etlDictionary";

  /** Name of the dataset that tracks progress of ETL tasks (e.g. for incremental processing) */
  public static final String ETL_META_DATASET = "etlMeta";

  /** Runtime argument that holds unique task id */
  public static final String ARG_TASK_ID = "etl.taskId";

  /** Name of the stream to be used by flow upon deploying an app. Actual stream source is set after deploy */
  public static final String DEFAULT_INPUT_STREAM = "default_stream";

  /** Batch processing configuration option names */
  public static final class Batch {

    public static final class Sink {
      public static final String ARG_SINK_TYPE = "etl.mapreduce.sink.type";
      public static final String ARG_SINK_SCHEMA = "etl.sink.mr.schema";

      public static final class Kafka {
        public static final String ARG_KAFKA_TOPIC = "etl.sink.mr.kafka.topic";
        public static final String ARG_KAFKA_PARTITION_FIELD = "etl.sink.mr.kafka.partition.field";
        public static final String ARG_KAFKA_ZOOKEEPER_QUORUM = "etl.sink.mr.kafka.zookeeper.quorum";
      }

      public static final class HBase {
        public static final String ARG_TABLE_NAME = "etl.sink.mr.hbase.table.name";
        public static final String ARG_ROW_KEY_FIELD = "etl.sink.mr.hbase.row.key.field";
        public static final String ARG_TABLE_FAMILY = "etl.sink.mr.hbase.table.family";
        public static final String ARG_HBASE_ZOOKEEPER_QUORUM = "etl.sink.mr.hbase.zookeeper.quorum";
        public static final String ARG_HBASE_ZOOKEEPER_CLIENT_PORT = "etl.sink.mr.hbase.zookeeper.client.port";
        public static final String ARG_HBASE_ZOOKEEPER_PARENT_NODE = "etl.sink.mr.hbase.zookeeper.parent.node";
      }

      public static final class Hive {
        public static final String ARG_HIVE_DB = "etl.sink.mr.hive.db";
        public static final String ARG_HIVE_TABLE = "etl.sink.mr.hive.table";
        public static final String ARG_HIVE_METASTORE_URI = "etl.sink.mr.hive.metastoreURI";
        public static final String ARG_HIVE_SERVER_URI = "etl.sink.mr.hive.hiveServerURI";
        public static final String ARG_PARTITIONS = "etl.sink.mr.hive.partitionValues";
        public static final String ARG_BASE_DIR = "etl.sink.mr.hive.basedir";
      }

      public static final class Dictionary {
        public static final String ARG_DICTIONARY_NAME = "etl.sink.mr.dictionary.name";
        public static final String ARG_DICTIONARY_KEY_FIELD = "etl.sink.mr.dictionary.keyField";
      }
    }

    public static final class Source {
      public static final String ARG_SOURCE_TYPE = "etl.mapreduce.source.type";
      public static final String ARG_SOURCE_SCHEMA = "etl.source.mr.schema";

      public static final class Stream {
        public static final String ARG_INPUT_STREAM = "etl.source.mr.stream.id";
        public static final String ARG_FIELD_SEPARATOR = "etl.source.mr.stream.fieldSeparator";
        public static final String ARG_RECORD_SEPARATOR = "etl.source.mr.stream.recordSeparator";
      }

      public static final class Table {
        public static final String ARG_INPUT_TABLE = "etl.source.mr.table.name";
      }
    }

    public static final class Transformation {
      public static final String ARG_TRANSFORMATION_TYPE = "etl.mapreduce.transformation.type";
    }

  }

  /** Realtime processing configuration option names */
  public static final class Realtime {

    public static final class Sink {
      public static final String ARG_SINK_SCHEMA = "etl.sink.realtime.schema";
      public static final String ARG_SINK_TYPE = "etl.realtime.sink.type";

      public static final class Kafka {
        public static final String ARG_KAFKA_TOPIC = "etl.sink.realtime.kafka.topic";
        public static final String ARG_KAFKA_PARTITION_FIELD = "etl.sink.realtime.kafka.partition.field";
        public static final String ARG_KAFKA_ZOOKEEPER_QUORUM = "etl.sink.realtime.kafka.zookeeper.quorum";
      }

      public static final class HBase {
        public static final String ARG_HBASE_ZOOKEEPER_QUORUM = "etl.sink.realtime.hbase.zookeeper.quorum";
        public static final String ARG_HBASE_ZOOKEEPER_CLIENT_PORT = "etl.sink.realtime.hbase.zookeeper.client.port";
        public static final String ARG_HBASE_ZOOKEEPER_PARENT_NODE = "etl.sink.realtime.hbase.zookeeper.parent.node";
        public static final String ARG_TABLE_NAME = "etl.sink.realtime.hbase.table.name";
        public static final String ARG_COLFAM = "etl.sink.realtime.hbase.table.colfam";
        public static final String ARG_ROW_KEY_FIELD = "etl.sink.realtime.hbase.row.key.field";
      }

      public static final class Dictionary {
        public static final String ARG_DICTIONARY_NAME = "etl.sink.realtime.dictionary.name";
        public static final String ARG_DICTIONARY_KEY_FIELD = "etl.sink.realtime.dictionary.keyField";
      }
    }

    public static final class Source {
      public static final String ARG_SOURCE_SCHEMA = "etl.source.realtime.schema";
      public static final String ARG_SOURCE_TYPE = "etl.realtime.source.type";

      public static final class Stream {
        public static final String ARG_INPUT_STREAM = "etl.source.realtime.stream.id";
        public static final String ARG_FIELD_SEPARATOR = "etl.source.realtime.stream.fieldSeparator";
        public static final String ARG_RECORD_SEPARATOR = "etl.source.realtime.stream.recordSeparator";
      }

      public static final class Metadata {
        public static final String SIZE_FIELD = "size.bytes";
      }
    }

    public static final class Transformation {
      public static final String ARG_TRANSFORMATION_TYPE = "etl.realtime.transformation.type";
    }
  }

  public static final class Transformation {

    public static final class Schema {
      public static final String ARG_INPUT_SCHEMA = "etl.transform.schema.input";
      public static final String ARG_OUTPUT_SCHEMA = "etl.transform.schema.output";
    }

    public static final class SchemaMapping {
      public static final String ARG_MAPPING = "etl.transform.schema.mapping";
    }
  }

  private Constants() {}
}
