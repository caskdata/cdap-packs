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

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

public class UniqueCountTest extends TestBase {
  private static final long testStartTime = 1417325400;

  private static final long getTime(int n) {
    return testStartTime + n * Constants.LOWEST_RESOLUTION_SECONDS;
  }

  @Test
  public void testCount() throws Exception {
    ApplicationManager applicationManager = deployApplication(UniquesApps.class);
    DataSetManager<UniqueCount> dataSetManager = applicationManager.getDataSet("unique_count");
    UniqueCount uniqueCountDataset = dataSetManager.get();

    // Adding events to unique_count Dataset.
    uniqueCountDataset.add("UNIQUE_USERS", getTime(0), new Object[] { "A", "B", "C", "D" });      // Start ts
    uniqueCountDataset.add("UNIQUE_USERS", getTime(1), new Object[] { "E", "F", "G", "H" });      // ts + 5 min
    uniqueCountDataset.add("UNIQUE_USERS", getTime(2), new Object[] { "I", "J" });                // ts + 10 min
    uniqueCountDataset.add("UNIQUE_USERS", getTime(3), new Object[] { "K" });                     // ts + 15 min
    uniqueCountDataset.add("UNIQUE_USERS", getTime(4), new Object[] { "L", "M", "N" });           // ts + 20 min
    uniqueCountDataset.add("UNIQUE_USERS", getTime(5), new Object[] { "O", "P", "C", "D" });      // ts + 25 min
    uniqueCountDataset.add("UNIQUE_USERS", getTime(6), new Object[] { "Q", "R", "S", "T", "U" }); // ts + 30 min

    // Querying based on time.
    List<Result> result;

    // 4 Unique users between ts >= getTime(0) & ts < getTime(1)
    result = uniqueCountDataset.query("UNIQUE_USERS", getTime(0), getTime(1), Resolution.FIVE_MIN);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.get(0).getCount() == 4);

    // 8 Unique users between ts >= getTime(0) & ts < getTime(2). There are two rows. Five min resolution.
    result = uniqueCountDataset.query("UNIQUE_USERS", getTime(0), getTime(2), Resolution.FIVE_MIN);
    Assert.assertTrue(result.size() == 2);
    Assert.assertTrue(result.get(0).getCount() == 4);
    Assert.assertTrue(result.get(1).getCount() == 4);

    // 8 Unique users between ts >= getTime(0) & ts < getTime(2). There is one row. Ten min resolution.
    result = uniqueCountDataset.query("UNIQUE_USERS", getTime(0), getTime(2), Resolution.TEN_MIN);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.get(0).getCount() == 8);

    // Clear the environment.
    clear();
  }


  @Test
  public void testCountsWithPartition() throws Exception {
    ApplicationManager applicationManager = deployApplication(UniquesApps.class);
    DataSetManager<UniqueCount> dataSetManager = applicationManager.getDataSet("unique_count");
    UniqueCount uniqueCountDataset = dataSetManager.get();

    // Adding events to unique_count Dataset.
    uniqueCountDataset.add("UNIQUE_USERS", getTime(0), new Object[] { "A", "B", "C", "D" }, 1);
    uniqueCountDataset.add("UNIQUE_USERS", getTime(0), new Object[] { "E", "F", "G", "H" }, 2);
    uniqueCountDataset.add("UNIQUE_USERS", getTime(0), new Object[] { "I", "J", "A", "C", "D"}, 3); // Only 10 uniques

    uniqueCountDataset.add("UNIQUE_USERS", getTime(1), new Object[] { "K" }, 1);
    uniqueCountDataset.add("UNIQUE_USERS", getTime(1), new Object[] { "L", "M", "N", "K"}, 2);
    uniqueCountDataset.add("UNIQUE_USERS", getTime(1), new Object[] { "O", "P", "C", "D", "K", "L", "O", "N" }, 3);

    uniqueCountDataset.add("UNIQUE_USERS", getTime(2), new Object[] { "Q", "R", "S", "T", "U" });

    // Querying based on time.
    List<Result> result;

    // 4 Unique users between ts >= getTime(0) & ts < getTime(1)
    result = uniqueCountDataset.query("UNIQUE_USERS", getTime(0), getTime(1), Resolution.FIVE_MIN);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.get(0).getCount() == 10);

    // 8 Unique users between ts >= getTime(0) & ts < getTime(2). There are two rows. Five min resolution.
    result = uniqueCountDataset.query("UNIQUE_USERS", getTime(1), getTime(2), Resolution.FIVE_MIN);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.get(0).getCount() == 8);

    // 16 Unique users between ts >= getTime(0) & ts < getTime(3).
    result = uniqueCountDataset.query("UNIQUE_USERS", getTime(0), getTime(3), Resolution.TEN_MIN);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.get(0).getCount() == 16);

    // 21 Unique users between ts >= getTime(0) & ts < getTime(3).
    result = uniqueCountDataset.query("UNIQUE_USERS", getTime(0), getTime(4), Resolution.FIFTEEN_MIN);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.get(0).getCount() == 21);

    // Clear the environment.
    clear();
  }
}