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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.batch.sink.MapReduceSink;
import co.cask.cdap.packs.etl.batch.source.MapReduceSource;
import co.cask.cdap.packs.etl.transform.Transformation;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Map-reduce program that performs ETL work
 */
public class ETLMapReduce extends AbstractMapReduce {
  public static final String NAME = "ETLMapReduce";
  // Optional: will be resolved via args
  private MapReduceSource source = null;
  private Transformation transformation = null;
  private MapReduceSink sink = null;

  public ETLMapReduce(MapReduceSource source, Transformation transformation, MapReduceSink sink) {
    this.source = source;
    this.transformation = transformation;
    this.sink = sink;
  }

  @Override
  public void configure() {
    Map<String, String> args = Maps.newHashMap();
    if (source != null) {
      args.put(Constants.Batch.Source.ARG_SOURCE_TYPE, source.getClass().getName());
      args.putAll(source.getConfiguration());
    }
    if (transformation != null) {
      args.put(Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE, transformation.getClass().getName());
      args.putAll(transformation.getConfiguration());
    }
    if (sink != null) {
      args.put(Constants.Batch.Sink.ARG_SINK_TYPE, sink.getClass().getName());
      args.putAll(sink.getConfiguration());
    }

    setName(NAME);
    setDescription("");
    setProperties(args);
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();

    // Needed since we use Guava 13, but hadoop classpath may already have Guava 11 or other version
    job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

    setTransformation(job, context);
    setSource(job, context);
    setSink(job, context);

    job.setMapperClass(MapperImpl.class);

    job.setNumReduceTasks(0);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    getSource(context).onFinish(succeeded, context);
  }

  private void setSource(Job job, MapReduceContext context) throws Exception {
    MapReduceSource source = getSource(context);
    source.prepareJob(context);
    job.getConfiguration().set(Constants.Batch.Source.ARG_SOURCE_TYPE, source.getClass().getName());
  }

  private void setTransformation(Job job, MapReduceContext context) throws Exception {
    Transformation transformation = getTransformation(context);
    job.getConfiguration().set(Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE, transformation.getClass().getName());
  }

  private void setSink(Job job, MapReduceContext context) throws Exception {
    MapReduceSink dest = getSink(context);
    dest.prepareJob(context);
    job.getConfiguration().set(Constants.Batch.Sink.ARG_SINK_TYPE, dest.getClass().getName());
  }

  /**
   * Override it to provide different transformation logic.
   * The default implementation is using program runtime argument {@link co.cask.cdap.packs.etl.Constants.Batch.Transformation#ARG_TRANSFORMATION_TYPE}
   * to get the name of
   * a class implementing transformation logic. If this method is not overridden this runtime argument is required
   * @param context instance of {@link MapReduceContext}
   * @return instance of {@link Transformation} to be used for transformation
   * @throws Exception
   */
  protected Transformation getTransformation(MapReduceContext context) throws Exception {
    String transformationType = Programs.getArgOrProperty(context, Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE);
    Preconditions.checkArgument(transformationType != null, "Missing required runtime argument " + Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE);

    return (Transformation) Class.forName(transformationType).newInstance();
  }

  /**
   * Override it to provide different source.
   * The default implementation is using program runtime argument {@link co.cask.cdap.packs.etl.Constants.Batch.Source#ARG_SOURCE_TYPE} to get
   * the name of a class implementing source. If this method is not overridden this runtime argument is required
   * @param context instance of {@link MapReduceContext}
   * @return instance of {@link MapReduceSource} to be used as source
   * @throws Exception
   */
  protected MapReduceSource getSource(MapReduceContext context) throws Exception {
    String sourceType = Programs.getArgOrProperty(context, Constants.Batch.Source.ARG_SOURCE_TYPE);
    Preconditions.checkArgument(sourceType != null, "Missing required runtime argument " + Constants.Batch.Source.ARG_SOURCE_TYPE);

    return (MapReduceSource) Class.forName(sourceType).newInstance();
  }

  /**
   * Override it to provide different sink.
   * The default implementation is using program runtime argument {@link co.cask.cdap.packs.etl.Constants.Batch.Sink#ARG_SINK_TYPE} to get the
   * name of a class implementing sink. If this method is not overridden this runtime argument is required
   * @param context instance of {@link MapReduceContext}
   * @return instance of {@link co.cask.cdap.packs.etl.batch.sink.MapReduceSink} to be used as sink
   * @throws Exception
   */
  protected MapReduceSink getSink(MapReduceContext context) throws Exception {
    String sinkType = Programs.getArgOrProperty(context, Constants.Batch.Sink.ARG_SINK_TYPE);
    Preconditions.checkArgument(sinkType != null,
                                "Missing required runtime argument " + Constants.Batch.Sink.ARG_SINK_TYPE);

    return (MapReduceSink) Class.forName(sinkType).newInstance();
  }

  public static class MapperImpl extends Mapper implements ProgramLifecycle<MapReduceContext> {
    private MapReduceContext mrContext;
    private MapReduceSource src;
    private Transformation transformation;
    private MapReduceSink dest;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      try {
        src = init(context, Constants.Batch.Source.ARG_SOURCE_TYPE);
        transformation = init(context, Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE);
        dest = init(context, Constants.Batch.Sink.ARG_SINK_TYPE);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void initialize(MapReduceContext mrContext) throws Exception {
      this.mrContext = mrContext;
    }

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      Iterator<Record> ins = src.read(key, value);
      while (ins.hasNext()) {
        Record in = ins.next();
        @Nullable
        Record out = transformation.transform(in);
        if (out != null) {
          dest.write(context, out);
        }
      }
    }

    @Override
    public void destroy() {
      src.destroy();
      transformation.destroy();
      dest.destroy();
    }

    private <T extends ProgramLifecycle> T init(Context context, String typeProperty) throws Exception {
      String typeName = context.getConfiguration().get(typeProperty);
      ProgramLifecycle<MapReduceContext> configurable;
      configurable = (ProgramLifecycle<MapReduceContext>) Class.forName(typeName).newInstance();
      configurable.initialize(mrContext);

      return (T) configurable;
    }

  }
}
