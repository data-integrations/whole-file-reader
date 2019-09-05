/*
 * Copyright © 2019 CDAP.
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

package io.cdap;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.format.WholeFileInputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A source that uses the {@link WholeFileInputFormat} to read whole file as one record.
 *
 * TODO: Move this to hydrator-plugins and make it extends from FileBatchSource to get the full configurability.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("WholeFileReader")
@Description("Reads content of the whole file as one record")
public class WholeFileSource extends BatchSource<String, BytesWritable, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(WholeFileSource.class);

  private final WholeFileSourceConfig config;
  private final Schema outputSchema;

  public WholeFileSource(WholeFileSourceConfig config) {
    this.config = config;
    this.outputSchema = createOutputSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    FailureCollector failureCollector = configurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
    configurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
    Job job = createJob();

    FileInputFormat.setInputPaths(job, config.getPath());
    final String inputDir = job.getConfiguration().get(FileInputFormat.INPUT_DIR);

    context.setInput(Input.of(config.getReferenceName(), new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return WholeFileInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.singletonMap(FileInputFormat.INPUT_DIR, inputDir);
      }
    }));
  }

  @Override
  public void transform(KeyValue<String, BytesWritable> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecord.builder(outputSchema)
                   .set("filePath", input.getKey())
                   .set("body", input.getValue().getBytes()).build());
  }

  private Schema createOutputSchema() {
    return Schema.recordOf("output", Schema.Field.of("filePath", Schema.of(Schema.Type.STRING)),
                           Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
  }

  private static Job createJob() throws IOException {
    try {
      Job job = Job.getInstance();

      LOG.info("Job new instance");

      // some input formats require the credentials to be present in the job. We don't know for
      // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
      // effect, because this method is only used at configure time and will be ignored later on.
      if (UserGroupInformation.isSecurityEnabled()) {
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        job.getCredentials().addAll(credentials);
      }

      return job;
    } catch (Exception e) {
      LOG.error("Exception ", e);
      throw e;
    }
  }

}
