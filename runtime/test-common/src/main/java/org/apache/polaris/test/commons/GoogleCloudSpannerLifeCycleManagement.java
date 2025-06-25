/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.test.commons;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.ImmutableList;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

public class GoogleCloudSpannerLifeCycleManagement
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  private static Logger LOGGER =
      LoggerFactory.getLogger(GoogleCloudSpannerLifeCycleManagement.class);

  private SpannerEmulatorContainer spannerContainer;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {}

  public GoogleCloudSpannerLifeCycleManagement() {}

  @Override
  public Map<String, String> start() {
    spannerContainer =
        new SpannerEmulatorContainer(
            DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator"));
    spannerContainer.start();
    Spanner spanner =
        SpannerOptions.newBuilder()
            .setEmulatorHost(spannerContainer.getEmulatorGrpcEndpoint())
            .setProjectId("emulator-project")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();

    InstanceId instanceId = InstanceId.of("emulator-project", "test-instance");
    DatabaseId databaseId = DatabaseId.of(instanceId, "test-database");

    try {

      spanner
          .getInstanceAdminClient()
          .createInstance(
              InstanceInfo.newBuilder(instanceId)
                  .setDisplayName("Test Instance")
                  .setNodeCount(1)
                  .setInstanceConfigId(
                      InstanceConfigId.of(instanceId.getProject(), "emulator-config"))
                  .build())
          .get();
      spanner
          .getDatabaseAdminClient()
          .createDatabase(
              databaseId.getInstanceId().getInstance(),
              databaseId.getDatabase(),
              ImmutableList.of())
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Unable to initialize Spanner Emulator", e);
    }

    return Map.of(
        "polaris.persistence.type",
        "google-cloud-spanner",
        "polaris.persistence.spanner.emulator-host",
        spannerContainer.getEmulatorGrpcEndpoint(),
        "polaris.persistence.spanner.database-id",
        databaseId.getName(),
        "polaris.persistence.spanner.bootstrap-schema",
        "true");
  }

  @Override
  public void stop() {}
}
