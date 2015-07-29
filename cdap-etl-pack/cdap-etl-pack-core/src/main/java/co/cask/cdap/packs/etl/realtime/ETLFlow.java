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

package co.cask.cdap.packs.etl.realtime;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.realtime.sink.RealtimeSink;
import co.cask.cdap.packs.etl.realtime.source.RealtimeSource;
import co.cask.cdap.packs.etl.transform.Transformation;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * {@link Flow} that performs ETL work
 */
public class ETLFlow extends AbstractFlow {
  // Optional: will be resolved via args
  private String inputStream = null;
  private RealtimeSource source = null;
  private Transformation transformation = null;
  private RealtimeSink sink = null;
  private Set<String> datasets = Collections.emptySet();

  public ETLFlow() {
  }

  public ETLFlow(String inputStream, RealtimeSource source, Transformation transformation,
                 RealtimeSink sink, Set<String> datasets) {
    this.inputStream = inputStream;
    this.source = source;
    this.transformation = transformation;
    this.sink = sink;
    this.datasets = datasets == null ? Collections.<String>emptySet() : datasets;
  }

  @Override
  public void configureFlow() {
    setName("ETLFlow");
    addFlowlet("ETLFlowlet", new ETLFlowlet(source, transformation, sink, datasets));
    connectStream(inputStream == null ? Constants.DEFAULT_INPUT_STREAM : inputStream, "ETLFlowlet");
  }

  public static final class ETLFlowlet extends AbstractFlowlet {
    // Optional: will be resolved via args
    private RealtimeSource source = null;
    private Transformation transformation = null;
    private RealtimeSink sink = null;
    private Set<String> datasets = Collections.emptySet();

    public ETLFlowlet(RealtimeSource source, Transformation transformation, RealtimeSink sink, Set<String> datasets) {
      this.source = source;
      this.transformation = transformation;
      this.sink = sink;
      this.datasets = datasets;
    }

    @Override
    public void configure(FlowletConfigurer configurer) {
      super.configure(configurer);
      Map<String, String> args = Maps.newHashMap();
      if (source != null) {
        args.put(Constants.Realtime.Source.ARG_SOURCE_TYPE, source.getClass().getName());
        args.putAll(source.getConfiguration());
      }
      if (transformation != null) {
        args.put(Constants.Realtime.Transformation.ARG_TRANSFORMATION_TYPE, transformation.getClass().getName());
        args.putAll(transformation.getConfiguration());
      }
      if (sink != null) {
        args.put(Constants.Realtime.Sink.ARG_SINK_TYPE, sink.getClass().getName());
        args.putAll(sink.getConfiguration());
      }

      setName("ETLFlowlet");
      setProperties(args);
      useDatasets(Constants.DICTIONARY_DATASET);
      useDatasets(datasets);
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      source = getSource(context);
      source.initialize(context);
      transformation = getTransformation(context);
      transformation.initialize(context);
      sink = getSink(context);
      sink.initialize(context);
    }

    @ProcessInput
    public void process(StreamEvent event) throws Exception {
      Iterator<Record> inputs = source.read(event);
      while (inputs.hasNext()) {
        Record input = inputs.next();
        @Nullable
        Record output = transformation.transform(input);
        if (output != null) {
          sink.write(output);
        }
      }
    }

    @Override
    public void destroy() {
      source.destroy();
      transformation.destroy();
      sink.destroy();
    }

    /**
     * Override it to provide different transformation logic.
     * The default implementation is using program runtime argument
     * {@link co.cask.cdap.packs.etl.Constants.Realtime.Transformation#ARG_TRANSFORMATION_TYPE} to get the name of
     * a class implementing transformation logic. If this method is not overridden this runtime argument is required
     * @param context instance of {@link FlowletContext}
     * @return instance of {@link co.cask.cdap.packs.etl.transform.Transformation} to be used for transformation
     * @throws Exception
     */
    protected Transformation getTransformation(FlowletContext context) throws Exception {
      String transformationType =
        Programs.getArgOrProperty(context, Constants.Realtime.Transformation.ARG_TRANSFORMATION_TYPE);
      Preconditions.checkArgument(transformationType != null,
                                  "Missing required runtime argument " + Constants.Realtime.Transformation.ARG_TRANSFORMATION_TYPE);

      return (Transformation) Class.forName(transformationType).newInstance();
    }

    /**
     * Override it to provide different source.
     * The default implementation is using program runtime argument {@link co.cask.cdap.packs.etl.Constants.Realtime.Source#ARG_SOURCE_TYPE}
     * to get the name of a class implementing source. If this method is not overridden this runtime argument is required
     * @param context instance of {@link FlowletContext}
     * @return instance of {@link co.cask.cdap.packs.etl.realtime.source.RealtimeSource} to be used as source
     * @throws Exception
     */
    protected RealtimeSource getSource(FlowletContext context) throws Exception {
      String sourceType = Programs.getArgOrProperty(context, Constants.Realtime.Source.ARG_SOURCE_TYPE);
      Preconditions.checkArgument(sourceType != null,
                                  "Missing required runtime argument " + Constants.Realtime.Source.ARG_SOURCE_TYPE);

      return (RealtimeSource) Class.forName(sourceType).newInstance();
    }

    /**
     * Override it to provide different sink.
     * The default implementation is using program runtime argument {@link co.cask.cdap.packs.etl.Constants.Realtime.Sink#ARG_SINK_TYPE}
     * to get the name of
     * a class implementing sink. If this method is not overridden this runtime argument is required
     * @param context instance of {@link FlowletContext}
     * @return instance of {@link co.cask.cdap.packs.etl.realtime.sink.RealtimeSink} to be used as sink
     * @throws Exception
     */
    protected RealtimeSink getSink(FlowletContext context) throws Exception {
      String sinkType = Programs.getArgOrProperty(context, Constants.Realtime.Sink.ARG_SINK_TYPE);
      Preconditions.checkArgument(sinkType != null,
                                  "Missing required runtime argument " + Constants.Realtime.Sink.ARG_SINK_TYPE);

      return (RealtimeSink) Class.forName(sinkType).newInstance();
    }


  }
}
