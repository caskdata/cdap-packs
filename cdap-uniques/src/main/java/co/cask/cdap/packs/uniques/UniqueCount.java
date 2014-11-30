/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.packs.uniques;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.util.Lists;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * UniqueCount Dataset
 */
public final class UniqueCount extends AbstractDataset implements RecordScannable<UniqueCountRecord> {

  private Table uniques;

  public UniqueCount(DatasetSpecification spec, @EmbeddedDataset("uniques") Table uniques) {
    super(spec.getName(), uniques);
    this.uniques = uniques;
  }

  public void add(String dimension, long timestamp, Object[] objects) throws Exception {
    add(dimension, timestamp, objects, 1);
  }

  public void add(String dimension, long timestamp, Object[] objects, int partition) throws IOException {
    long boxedTimestamp = boxedTimestamp(timestamp);
    byte[] row = makeRowKey(dimension, boxedTimestamp);
    byte[] col = makeColumn(partition);
    byte[] hll = uniques.get(row, col);
    HyperLogLogPlus hllplus;
    if(hll == null) {
      hllplus = new HyperLogLogPlus(24,25);
    } else {
      hllplus = HyperLogLogPlus.Builder.build(hll);
    }
    for(Object o : objects ) {
      hllplus.offer(o);
    }
    uniques.put(row, col, hllplus.getBytes());
  }

  public List<Result> query(String dim, long start, long end, Resolution resolution) throws IOException {
    boolean isSegmentStartHLL = true;
    long segmentTimestamp = 0L;
    List<Result> result = Lists.newArrayList();
    Row currentRow = null;
    ICardinality segmentHLL = null;

    byte[] rowStart = makeRowKey(dim, boxedTimestamp(start));
    byte[] rowEnd = makeRowKey(dim, boxedTimestamp(end));

    long segmentCount = resolution.getResolution() / Constants.LOWEST_RESOLUTION_SECONDS;
    long segmentCounter = segmentCount;

    Scanner scanner = uniques.scan(rowStart, rowEnd);
    while((currentRow = scanner.next()) != null) {
      Map<byte[], byte[]> allColumns = currentRow.getColumns();
      ICardinality partitionHLL = null;
      for(Map.Entry<byte[], byte[]> column : allColumns.entrySet()) {
        HyperLogLogPlus h3 = HyperLogLogPlus.Builder.build(column.getValue());
        if ( partitionHLL == null ) {
          partitionHLL = h3;
        } else {
          try {
            partitionHLL = partitionHLL.merge(h3);
          } catch (CardinalityMergeException e) {
            continue;
          }
        }
      }

      HyperLogLogPlus h1 = HyperLogLogPlus.Builder.build(partitionHLL.getBytes());

      if (isSegmentStartHLL) {
        segmentHLL = h1;
        isSegmentStartHLL = false;
        segmentTimestamp = getRowTimestamp(currentRow.getRow());
        segmentCounter--;
      } else {
        if (segmentCounter > 0) {
          try {
            segmentHLL = segmentHLL.merge(h1);
            segmentCounter--;
          } catch (CardinalityMergeException e) {
            segmentCounter--;
            continue;
          }
        }
      }

      if (segmentCounter == 0) {
        segmentCounter = segmentCount;
        isSegmentStartHLL = true;
        result.add(new Result(segmentTimestamp, segmentHLL.cardinality()));
      }
    }
    return result;
  }


  private long boxedTimestamp(long timestamp) {
    return (long) Math.floor(timestamp  - (timestamp % Constants.LOWEST_RESOLUTION_SECONDS));
  }

  private long getRowTimestamp(byte[] row) {
    String k = Bytes.toString(row);
    String k1 = k.substring(k.length() - 10);
    return Long.parseLong(k1);
  }

  private byte[] makeRowKey(String dim, long boxedTimestamp) {
    String k = String.format("%s:%d", dim, boxedTimestamp);
    return Bytes.toBytes(k);
  }

  private byte[] makeColumn(int partition) {
    String column = String.format("%d:%d", Constants.LOWEST_RESOLUTION_SECONDS, partition);
    return column.getBytes();
  }

  @Override
  public Type getRecordType() {
    return UniqueCountRecord.class;
  }

  @Override
  public List<Split> getSplits() {
    return uniques.getSplits();
  }

  @Override
  public RecordScanner<UniqueCountRecord> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(uniques.createSplitReader(split),
                                         new Scannables.RecordMaker<byte[], Row, UniqueCountRecord>() {
      @Override
      public UniqueCountRecord makeRecord(byte[] bytes, Row row) {
        String rowKey = Bytes.toString(bytes);
        int length = rowKey.length();
        String timeStamp = rowKey.substring(length - 10);
        String dimension = rowKey.substring(0, length - 11);
        long cardinality = 0;
        try {
          byte[] hllBytes = row.get(Constants.COLUMN_NAME);
          if (hllBytes != null) {
            HyperLogLogPlus hll = HyperLogLogPlus.Builder.build(hllBytes);
            cardinality = hll.cardinality();
          }
        } catch (IOException e) {
          cardinality = -1;
        }
        return new UniqueCountRecord(dimension, Long.parseLong(timeStamp), cardinality);
      }
    });
  }
}
