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

package co.cask.cdap.packs.etl.batch;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.DatasetsTrackingApplication;
import co.cask.cdap.packs.etl.batch.sink.MapReduceSink;
import co.cask.cdap.packs.etl.batch.source.MapReduceSource;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.packs.etl.transform.Transformation;

/**
 * Simple application for Batch processing ETL pipeline.
 */
public class BatchETL extends DatasetsTrackingApplication {

  public void configure() {
    //setting defaults - can be overriden in configure()
    setName(this.getClass().getSimpleName());
    setDescription("Batch ETL Application");

    addStream(new Stream(Constants.DEFAULT_INPUT_STREAM));
    createDataset(Constants.DICTIONARY_DATASET, DictionaryDataSet.class);
    createDataset(Constants.ETL_META_DATASET, ETLMetaDataSet.class);

    Configurer configurer = new Configurer();
    configure(configurer);

    ETLMapReduce mapReduce = new ETLMapReduce(configurer.source,
                                              configurer.transformation,
                                              configurer.sink,
                                              getDatasets());

    // todo: this seems redundant: we add it as part of workflow
    addMapReduce(mapReduce);
    addWorkflow(new ETLMapReduceWorkflow(ETLMapReduce.NAME));

    long intervalInMinutes = 10;
    String cronEntry = "0/" + intervalInMinutes + " * * * *";
    scheduleWorkflow("FiveMinuteSchedule", cronEntry, ETLMapReduceWorkflow.NAME);
  }

  protected void configure(Configurer configurer) {
    // do nothing by default
  }

  public static final class Configurer {
    private MapReduceSource source = null;
    private Transformation transformation = null;
    private MapReduceSink sink = null;

    public void setSource(MapReduceSource source) {
      this.source = source;
    }

    public void setTransformation(Transformation transformation) {
      this.transformation = transformation;
    }

    public void setSink(MapReduceSink sink) {
      this.sink = sink;
    }
  }
}
