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
  public void testUniqueCountQuery() throws Exception {
    ApplicationManager applicationManager = deployApplication(UniquesApps.class);
    DataSetManager<UniqueCount> dataSetManager = applicationManager.getDataSet("unique_count");
    UniqueCount uniqueCountDataset = dataSetManager.get();

    // Adding events to unique_count Dataset.
    uniqueCountDataset.add("UNIQUE_IPS", getTime(0), new Object[] { "1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4" });
    uniqueCountDataset.add("UNIQUE_IPS", getTime(1), new Object[] { "5.5.5.5", "6.6.6.6", "7.7.7.7", "4.5.56.67" });
    uniqueCountDataset.add("UNIQUE_IPS", getTime(2), new Object[] { "3.4.5.6", "6.7.8.4" });
    uniqueCountDataset.add("UNIQUE_IPS", getTime(3), new Object[] { "2.3.4.5", "4.4.4.4" });
    uniqueCountDataset.add("UNIQUE_IPS", getTime(4), new Object[] { "3.4.2.1", "8.4.2.4", "4.3.2.4" });
    uniqueCountDataset.add("UNIQUE_IPS", getTime(5), new Object[] { "4.3.2.2", "6.7.7.3", "1.2.0.1", "127.0.0.1" });
    uniqueCountDataset.add("UNIQUE_IPS", getTime(6), new Object[] { "127.0.0.2", "67.89.2.4", "192.158.2.1",
      "192.168.10.1", "10.1.12.1" });

    // Flush all the data.
    dataSetManager.flush();

    Connection queryClient = getQueryClient();
    ResultSet results = null;
    try {
      results = queryClient.prepareStatement("SELECT * FROM cdap_user_unique_count ORDER by timestamp").executeQuery();
      Assert.assertNotNull(results);
//      results.next(); Assert.assertTrue(results.getLong(3) == 4L);
//      results.next(); Assert.assertTrue(results.getLong(3) == 4L);
//      results.next(); Assert.assertTrue(results.getLong(3) == 2L);
//      results.next(); Assert.assertTrue(results.getLong(3) == 2L);
//      results.next(); Assert.assertTrue(results.getLong(3) == 3L);
//      results.next(); Assert.assertTrue(results.getLong(3) == 4L);
//      results.next(); Assert.assertTrue(results.getLong(3) == 5L);
    } finally {
      results.close();
      queryClient.close();
    }
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
    uniqueCountDataset.add("UNIQUE_USERS", getTime(0), new Object[] { "I", "J" }, 3);
    uniqueCountDataset.add("UNIQUE_USERS", getTime(1), new Object[] { "K" }, 1);                     // ts + 15 min
    uniqueCountDataset.add("UNIQUE_USERS", getTime(1), new Object[] { "L", "M", "N" }, 2);           // ts + 20 min
    uniqueCountDataset.add("UNIQUE_USERS", getTime(1), new Object[] { "O", "P", "C", "D" }, 3);      // ts + 25 min
    uniqueCountDataset.add("UNIQUE_USERS", getTime(2), new Object[] { "Q", "R", "S", "T", "U" }); // ts + 30 min

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

    // Clear the environment.
    clear();
  }
}