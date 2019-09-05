/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;

/**
 * Configurations for the {@link WholeFileSource} plugin.
 */
public class WholeFileSourceConfig extends PluginConfig {

  public static final String PATH_PROPERTY_NAME = "path";

  @Description(
    "This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc."
  )
  private String referenceName;

  @Description(
    "Path to file(s) to be read. If a directory is specified, " +
      "terminate the path name with a \'/\'. For distributed file system such as HDFS, file system name should come" +
      " from 'fs.DefaultFS' property in the 'core-site.xml'. For example, 'hdfs://mycluster.net:8020/input', where" +
      " value of the property 'fs.DefaultFS' in the 'core-site.xml' is 'hdfs://mycluster.net:8020'. The path uses " +
      "filename expansion (globbing) to read files."
  )
  @Macro
  private String path;

  @Override
  public String toString() {
    return "WholeFileSourceConfig{" +
      "referenceName='" + referenceName + '\'' +
      ", path='" + path + '\'' +
      '}';
  }

  public String getReferenceName() {
    return referenceName;
  }

  public void setReferenceName(String referenceName) {
    this.referenceName = referenceName;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void validate(FailureCollector failureCollector) {
    try {
      IdUtils.validateId(referenceName);
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(e.getMessage(), null)
        .withConfigProperty(Constants.Reference.REFERENCE_NAME);
    }
    if (!containsMacro(PATH_PROPERTY_NAME)) {
      if (Strings.isNullOrEmpty(path)) {
        failureCollector.addFailure("Input path must be specified", null)
          .withConfigProperty(PATH_PROPERTY_NAME);
      }
    }
  }
}
