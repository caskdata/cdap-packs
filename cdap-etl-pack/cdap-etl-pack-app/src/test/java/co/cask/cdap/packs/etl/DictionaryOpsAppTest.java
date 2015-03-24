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

package co.cask.cdap.packs.etl;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.packs.etl.schema.FieldType;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Tests of Dictionary operations application.
 */
public class DictionaryOpsAppTest extends TestBase {

  private static final String DICT_SHAPES = "shapes";
  private static final String DICT_SHAPES_FIELD_AREA = "area";
  private static final String DICT_SHAPES_FIELD_COLOR = "color";
  private static final String DICT_SHAPES_FIELD_MIN = "min";
  private static final String DICT_SHAPES_FIELD_MAX = "max";

  private static final Gson GSON = new Gson();

  @Test
  public void testSerializeFieldType() {
    FieldType fieldType = GSON.fromJson("INT", FieldType.class);
    Assert.assertEquals(FieldType.INT, fieldType);
  }

  @Test
  public void testRetrieveDictionaryValue() throws Exception {
    // Deploy the DictionaryOpsApp
    ApplicationManager appManager = deployApplication(DictionaryOpsApp.class);

    DataSetManager<DictionaryDataSet> dictionary = getDataset(Constants.DICTIONARY_DATASET);
    dictionary.get().write(DICT_SHAPES, Bytes.toBytes("triangle"),
                           ImmutableMap.of(DICT_SHAPES_FIELD_AREA, FieldType.INT.toBytes(10),
                                           DICT_SHAPES_FIELD_COLOR, FieldType.STRING.toBytes("GREEN"),
                                           DICT_SHAPES_FIELD_MIN, FieldType.LONG.toBytes(Long.MIN_VALUE),
                                           DICT_SHAPES_FIELD_MAX, FieldType.DOUBLE.toBytes(Double.MAX_VALUE)));
    dictionary.get().write(DICT_SHAPES, Bytes.toBytes("circle"),
                           ImmutableMap.of(DICT_SHAPES_FIELD_AREA, FieldType.INT.toBytes(15),
                                           DICT_SHAPES_FIELD_COLOR, FieldType.STRING.toBytes("RED"),
                                           DICT_SHAPES_FIELD_MIN, FieldType.FLOAT.toBytes(Float.MIN_VALUE),
                                           DICT_SHAPES_FIELD_MAX, FieldType.LONG.toBytes(Long.MAX_VALUE)));

    // Makes changes of the dictionary dataset
    dictionary.flush();

    // Start the DictionaryOpsService
    ServiceManager serviceManager = appManager.startService(DictionaryOpsService.SERVICE_NAME);

    // Wait service startup
    serviceStatusCheck(serviceManager, true);

    // Get DictionaryOpsService URL
    URL url = serviceManager.getServiceURL();

    // Retrieve and verify data from the dictionary
    String response = requestService(new URL(url, "get/shapes/triangle/area/int"));
    Assert.assertEquals(10, Integer.valueOf(response).intValue());
    response = requestService(new URL(url, "get/shapes/triangle/color/string"));
    Assert.assertEquals("GREEN", response);
    response = requestService(new URL(url, "get/shapes/triangle/min/long"));
    Assert.assertEquals(Long.MIN_VALUE, Long.valueOf(response).longValue());
    response = requestService(new URL(url, "get/shapes/triangle/max/double"));
    Assert.assertEquals(Double.MAX_VALUE, Double.valueOf(response), 10.0d);

    response = requestService(new URL(url, "get/shapes/circle/area/int"));
    Assert.assertEquals(15, Integer.valueOf(response).intValue());
    response = requestService(new URL(url, "get/shapes/circle/color/string"));
    Assert.assertEquals("RED", response);
    response = requestService(new URL(url, "get/shapes/circle/min/float"));
    Assert.assertEquals(Float.MIN_VALUE, Float.valueOf(response), 10.0f);
    response = requestService(new URL(url, "get/shapes/circle/max/long"));
    Assert.assertEquals(Long.MAX_VALUE, Long.valueOf(response).longValue());

    // Test for incorrect type
    HttpURLConnection conn = (HttpURLConnection) (new URL(url, "get/shapes/triangle/area/object")).openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
    conn.disconnect();

    // Test for incorrect key
    conn = (HttpURLConnection) (new URL(url, "get/shapes/cube/area/int")).openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_NO_CONTENT, conn.getResponseCode());
    conn.disconnect();

    appManager.stopAll();
  }

  private String requestService(URL url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    try {
      return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }
}
