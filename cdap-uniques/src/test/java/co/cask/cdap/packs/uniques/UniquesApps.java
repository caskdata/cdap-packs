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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;

import javax.xml.crypto.Data;

/**
 * Test Application that includes the UniqueCount Dataset to be tested.
 */
public class UniquesApps extends AbstractApplication {
  @Override
  public void configure() {
    addService(new MockService());
    createDataset("unique_count", UniqueCount.class,
                  DatasetProperties.builder().add("conflict.level", ConflictDetection.COLUMN.name()).build());
  }

  public static class MockService extends AbstractService {
    public static final String NAME = "MockService";

    @Override
    protected void configure() {
      setName(NAME);
      addHandler(new NoOpHandler());
    }
  }
  /**
   * No-op handler
   */
  public static final class NoOpHandler extends AbstractHttpServiceHandler {

  }

}
