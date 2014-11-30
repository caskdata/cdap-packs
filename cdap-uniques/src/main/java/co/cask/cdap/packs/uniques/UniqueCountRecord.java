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

/**
 * Represents the object that the {@link co.cask.cdap.packs.uniques.UniqueCount} table is
 * represented as.
 */
public final class UniqueCountRecord {
  private final String dimension;
  private final long timestamp;
  private final long cardinality;

  public UniqueCountRecord(String dimension, long timestamp, long cardinality) {
    this.dimension = dimension;
    this.timestamp = timestamp;
    this.cardinality = cardinality;
  }

  public String getDimension() {
    return dimension;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getCardinality() {
    return cardinality;
  }
}
