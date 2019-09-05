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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test cases for {@link WholeFileSourceConfig}.
 */
public class WholeFileSourceConfigTest {
  @Test
  public void testValidateValidConfig() {
    WholeFileSourceConfig config = getValidConfig();

    MockFailureCollector failureCollector = new MockFailureCollector("test");
    config.validate(failureCollector);
    List<ValidationFailure> failures = failureCollector.getValidationFailures();
    Assert.assertEquals(0, failures.size());
  }

  @Test
  public void testValidateReference() {
    WholeFileSourceConfig config = getValidConfig();
    config.setReferenceName("");

    assertConfigValidationFailed(config, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidatePath() {
    WholeFileSourceConfig config = getValidConfig();
    config.setPath("");

    assertConfigValidationFailed(config, WholeFileSourceConfig.PATH_PROPERTY_NAME);
  }

  private static void assertConfigValidationFailed(WholeFileSourceConfig config, String propertyName) {
    MockFailureCollector failureCollector = new MockFailureCollector("test");
    config.validate(failureCollector);
    List<ValidationFailure> failures = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failures.size());
    ValidationFailure failure = failures.get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    ValidationFailure.Cause cause = causes.get(0);
    Assert.assertEquals(propertyName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private WholeFileSourceConfig getValidConfig() {
    WholeFileSourceConfig config = new WholeFileSourceConfig();
    config.setReferenceName("valid");
    config.setPath("valid");
    return config;
  }
}