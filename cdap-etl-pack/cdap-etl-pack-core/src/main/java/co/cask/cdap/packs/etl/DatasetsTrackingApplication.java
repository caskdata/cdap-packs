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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Helps define {@link AbstractApplication} class that tracks all datasets added to it.
 */
public abstract class DatasetsTrackingApplication extends AbstractApplication<Config> {
  // we need to call useDataset() in MR & Flow - otherwise it will not work, hence this code
  // todo: check that we have to have it
  private final Set<String> datasets = Sets.newHashSet();

  @Override
  protected void createDataset(String datasetName, String typeName) {
    super.createDataset(datasetName, typeName);
    datasets.add(datasetName);
  }

  @Override
  protected void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    super.createDataset(datasetName, typeName, properties);
    datasets.add(datasetName);
  }

  @Override
  protected void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    super.createDataset(datasetName, datasetClass, props);
    datasets.add(datasetName);
  }

  @Override
  protected void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    super.createDataset(datasetName, datasetClass);
    datasets.add(datasetName);
  }

  protected Set<String> getDatasets() {
    return datasets;
  }
}
