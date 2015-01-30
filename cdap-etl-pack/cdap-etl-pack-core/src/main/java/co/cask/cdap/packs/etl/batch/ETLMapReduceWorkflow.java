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

import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * Contains ETL MapReduce program
 */
public class ETLMapReduceWorkflow extends AbstractWorkflow {
  public static final String NAME = "ETLMapReduceWorkFlow";
  private String mapReduce = null;

  public ETLMapReduceWorkflow(String mapReduce) {
    this.mapReduce = mapReduce;
  }

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Performs incremental processing with ETL MapReduce");
    addMapReduce(mapReduce);
  }
}
