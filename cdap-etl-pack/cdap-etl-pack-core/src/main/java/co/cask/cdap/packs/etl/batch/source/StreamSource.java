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

package co.cask.cdap.packs.etl.batch.source;

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.batch.ETLMetaDataSet;
import co.cask.cdap.packs.etl.schema.Field;
import co.cask.cdap.packs.etl.schema.Schema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Stream source.
 */
public class StreamSource extends SchemaSource<LongWritable, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamSource.class);

  private static final String DEFAULT_FIELD_SEPARATOR = ",";
  private static final String DEFAULT_RECORD_SEPARATOR = "\n";

  // by default, if that's the first time we are processing, we'll start from an hour ago
  @VisibleForTesting
  public static final long TO_PROCESS_ON_START = TimeUnit.HOURS.toMillis(1);

  private String inputStream = null;
  private String fieldSeparator;
  private String recordSeparator;
  private String taskId;

  public StreamSource() {
  }

  public StreamSource(Schema schema, String inputStream) {
    super(schema);
    this.inputStream = inputStream;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (inputStream != null) {
      args.put(Constants.Batch.Source.Stream.ARG_INPUT_STREAM, inputStream);
    }
    if (taskId != null) {
      args.put(Constants.ARG_TASK_ID, taskId);
    }
    if (fieldSeparator != null) {
      args.put(Constants.Batch.Source.Stream.ARG_FIELD_SEPARATOR, fieldSeparator);
    }
    if (recordSeparator != null) {
      args.put(Constants.Batch.Source.Stream.ARG_RECORD_SEPARATOR, recordSeparator);
    }
    return args;
  }

  @Override
  public void initialize(MapReduceContext context) {
    super.initialize(context);

    inputStream = Programs.getArgOrProperty(context, Constants.Batch.Source.Stream.ARG_INPUT_STREAM);
    Preconditions.checkArgument(inputStream != null, "Missing required argument " + Constants.Batch.Source.Stream.ARG_INPUT_STREAM);
    fieldSeparator = Programs.getArgOrProperty(context, Constants.Batch.Source.Stream.ARG_FIELD_SEPARATOR, DEFAULT_FIELD_SEPARATOR);
    recordSeparator = Programs.getArgOrProperty(context, Constants.Batch.Source.Stream.ARG_RECORD_SEPARATOR,
                                                DEFAULT_RECORD_SEPARATOR);
    taskId = Programs.getArgOrProperty(context, Constants.ARG_TASK_ID);
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    super.prepareJob(context);

    inputStream = Programs.getArgOrProperty(context, Constants.Batch.Source.Stream.ARG_INPUT_STREAM);
    Preconditions.checkArgument(inputStream != null, "Missing required argument " + Constants.Batch.Source.Stream.ARG_INPUT_STREAM);
    fieldSeparator = Programs.getArgOrProperty(context, Constants.Batch.Source.Stream.ARG_FIELD_SEPARATOR, DEFAULT_FIELD_SEPARATOR);
    recordSeparator = Programs.getArgOrProperty(context, Constants.Batch.Source.Stream.ARG_RECORD_SEPARATOR, DEFAULT_RECORD_SEPARATOR);
    taskId = Programs.getArgOrProperty(context, Constants.ARG_TASK_ID);

    ETLMetaDataSet mds = context.getDataset(Constants.ETL_META_DATASET);
    Progress progress = mds.get(taskId, Progress.class);
    Progress newProgress = advanceProgress(progress, context.getLogicalStartTime());
    mds.set(taskId, newProgress);

    long startTime = newProgress.lastProcessed;
    long endTime = newProgress.lastAttempted;

    String inputStreamUri = String.format("stream://%s?start=%d&end=%d", inputStream, startTime, endTime);
    LOG.info("Set input to {}", inputStreamUri);
    context.setInput(inputStreamUri, (List<Split>) null);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    if (succeeded) {
      String id = Programs.getArgOrProperty(context, Constants.ARG_TASK_ID);
      ETLMetaDataSet mds = context.getDataset(Constants.ETL_META_DATASET);
      Progress progress = mds.get(id, Progress.class);
      // NOTE: we know progress is not null
      // setting lastProcessed to lastAttempted since attempt was successful
      mds.set(id, new Progress(progress.lastAttempted, progress.lastAttempted));
    }
  }

  @Override
  public Iterator<Record> read(LongWritable key, Text value, Schema schema) {
    ImmutableList.Builder<Record> records = ImmutableList.builder();

    String recordSeparator = getRecordSeparator();
    if (recordSeparator != null) {
      String data = value.toString();
      String[] recordDatas = data.split(recordSeparator);
      for (String recordData : recordDatas) {
        addRecord(records, recordData, schema);
      }
    } else {
      addRecord(records, value.toString(), schema);
    }

    return records.build().iterator();
  }

  private void addRecord(ImmutableList.Builder<Record> records, String data, Schema schema) {
    String[] values = data.split(getFieldSeparator());
    int i = 0;
    Record.Builder builder = new Record.Builder();
    for (Field f : schema.getFields()) {
      builder.add(f.getName(), f.getType().toBytes(values[i++]));
    }
    records.add(builder.build());
  }

  private String getFieldSeparator() {
    return fieldSeparator;
  }

  private String getRecordSeparator() {
    return recordSeparator;
  }

  @VisibleForTesting
  public Progress advanceProgress(Progress original, long logicalStartTime) {
    long endTime = logicalStartTime;
    long startTime = original == null ? endTime - TO_PROCESS_ON_START : original.lastProcessed;
    return new Progress(startTime, endTime);
  }

  @VisibleForTesting
  public static final class Progress {
    // we definitely processed up till this point
    private long lastProcessed;
    // last attempt to process was to this point
    private long lastAttempted;

    @VisibleForTesting
    public Progress(long lastProcessed, long lastAttempted) {
      this.lastProcessed = lastProcessed;
      this.lastAttempted = lastAttempted;
    }

    public long getLastProcessed() {
      return lastProcessed;
    }

    public long getLastAttempted() {
      return lastAttempted;
    }
  }
}
