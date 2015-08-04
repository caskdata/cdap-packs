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

package co.cask.cdap.packs.etl.transform.script;

import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.packs.etl.schema.FieldType;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class LookupFunctionTest {

  @Test
  public void shouldPass() {
    int wifiId = 123;
    byte[] wifiIdBytes = FieldType.INT.toBytes(wifiId);

    String someWifiName = "some_wifi_name";
    byte[] someWifiNameBytes = FieldType.STRING.toBytes(someWifiName);

    String dictionaryName = "wifi_venues";
    String fieldName = "wifi_name";

    DictionaryDataSet table = mock(DictionaryDataSet.class);
    when(table.get(eq(dictionaryName), eq(wifiIdBytes), eq(fieldName)))
      .thenReturn(someWifiNameBytes);

    Record record = new Record.Builder()
      .add("wifi_id", wifiIdBytes)
      .add("dummy", FieldType.DOUBLE.toBytes(123.123))
      .build();

    LookupFunction lookupFunction = new LookupFunction(table, record);

    Assert.assertEquals(someWifiName, lookupFunction.lookup(dictionaryName, "wifi_id", fieldName, "undefined"));
  }

}
