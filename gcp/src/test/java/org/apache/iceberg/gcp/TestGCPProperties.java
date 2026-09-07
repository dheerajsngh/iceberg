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
package org.apache.iceberg.gcp;

import static org.apache.iceberg.gcp.GCPProperties.GCS_NO_AUTH;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_TOKEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestGCPProperties {

  @Test
  public void testOAuthWithNoAuth() {
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                new GCPProperties(ImmutableMap.of(GCS_OAUTH2_TOKEN, "oauth", GCS_NO_AUTH, "true")))
        .withMessage(
            String.format(
                "Invalid auth settings: must not configure %s and %s",
                GCS_NO_AUTH, GCS_OAUTH2_TOKEN));

    GCPProperties gcpProperties =
        new GCPProperties(ImmutableMap.of(GCS_OAUTH2_TOKEN, "oauth", GCS_NO_AUTH, "false"));
    assertThat(gcpProperties.noAuth()).isFalse();
    assertThat(gcpProperties.oauth2Token()).get().isEqualTo("oauth");
    gcpProperties = new GCPProperties(ImmutableMap.of(GCS_NO_AUTH, "true"));
    assertThat(gcpProperties.noAuth()).isTrue();
    assertThat(gcpProperties.oauth2Token()).isNotPresent();
  }

  @Test
  public void refreshCredentialsEndpointSet() {
    GCPProperties gcpProperties =
        new GCPProperties(
            ImmutableMap.of(GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, "/v1/credentials"));
    assertThat(gcpProperties.oauth2RefreshCredentialsEnabled()).isTrue();
    assertThat(gcpProperties.oauth2RefreshCredentialsEndpoint())
        .isPresent()
        .get()
        .isEqualTo("/v1/credentials");
  }

  @Test
  public void refreshCredentialsEndpointSetButRefreshDisabled() {
    GCPProperties gcpProperties =
        new GCPProperties(
            ImmutableMap.of(
                GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
                "/v1/credentials",
                GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED,
                "false"));
    assertThat(gcpProperties.oauth2RefreshCredentialsEnabled()).isFalse();
    assertThat(gcpProperties.oauth2RefreshCredentialsEndpoint())
        .isPresent()
        .get()
        .isEqualTo("/v1/credentials");
  }

  @Test
  public void verifyChannelWritePropertiesDefaults() {
    GCPProperties gcpProperties = new GCPProperties();
    assertThat(gcpProperties.kmsKeyName()).isNull();
    assertThat(gcpProperties.checksumValidationEnabled())
        .isEqualTo(GCPProperties.GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_DEFAULT);
    assertThat(gcpProperties.channelWriteUploadType())
        .isEqualTo(GCPProperties.GCS_CHANNEL_WRITE_UPLOAD_TYPE_DEFAULT);
    assertThat(gcpProperties.channelWritePcuBufferCount())
        .isEqualTo(GCPProperties.GCS_CHANNEL_WRITE_PCU_BUFFER_COUNT_DEFAULT);
    assertThat(gcpProperties.channelWritePcuBufferCapacity())
        .isEqualTo(GCPProperties.GCS_CHANNEL_WRITE_PCU_BUFFER_CAPACITY_DEFAULT);
    assertThat(gcpProperties.channelWritePcuCleanupType())
        .isEqualTo(GCPProperties.GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_DEFAULT);
    assertThat(gcpProperties.channelWritePcuNamePrefix())
        .isEqualTo(GCPProperties.GCS_CHANNEL_WRITE_PCU_NAME_PREFIX_DEFAULT);
    assertThat(gcpProperties.channelWriteTemporaryPaths()).isNull();

    GCPProperties customProperties =
        new GCPProperties(
            ImmutableMap.<String, String>builder()
                .put(
                    GCPProperties.GCS_KMS_KEY_NAME,
                    "projects/p/locations/l/keyRings/r/cryptoKeys/k")
                .put(GCPProperties.GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_ENABLED, "true")
                .put(
                    GCPProperties.GCS_CHANNEL_WRITE_UPLOAD_TYPE,
                    GCPProperties.GCS_CHANNEL_WRITE_UPLOAD_TYPE_PARALLEL_COMPOSITE_UPLOAD)
                .put(GCPProperties.GCS_CHANNEL_WRITE_PCU_BUFFER_COUNT, "4")
                .put(
                    GCPProperties.GCS_CHANNEL_WRITE_PCU_BUFFER_CAPACITY,
                    String.valueOf(64 * 1024 * 1024))
                .put(
                    GCPProperties.GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE,
                    GCPProperties.GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_ON_SUCCESS)
                .put(GCPProperties.GCS_CHANNEL_WRITE_PCU_NAME_PREFIX, "iceberg-part-")
                .put(GCPProperties.GCS_CHANNEL_WRITE_TEMPORARY_PATHS, "/tmp/dir1,/tmp/dir2")
                .build());

    assertThat(customProperties.kmsKeyName())
        .isEqualTo("projects/p/locations/l/keyRings/r/cryptoKeys/k");
    assertThat(customProperties.checksumValidationEnabled()).isTrue();
    assertThat(customProperties.channelWriteUploadType())
        .isEqualTo(GCPProperties.GCS_CHANNEL_WRITE_UPLOAD_TYPE_PARALLEL_COMPOSITE_UPLOAD);
    assertThat(customProperties.channelWritePcuBufferCount()).isEqualTo(4);
    assertThat(customProperties.channelWritePcuBufferCapacity()).isEqualTo(64 * 1024 * 1024);
    assertThat(customProperties.channelWritePcuCleanupType())
        .isEqualTo(GCPProperties.GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_ON_SUCCESS);
    assertThat(customProperties.channelWritePcuNamePrefix()).isEqualTo("iceberg-part-");
    assertThat(customProperties.channelWriteTemporaryPaths())
        .containsExactly("/tmp/dir1", "/tmp/dir2");
  }

  @Test
  public void verifyKmsKeyNameGetterReturnsNull() {
    GCPProperties properties = new GCPProperties();
    assertThat(properties.kmsKeyName()).isNull();

    GCPProperties propertiesWithEmptyMap = new GCPProperties(ImmutableMap.of());
    assertThat(propertiesWithEmptyMap.kmsKeyName()).isNull();
  }
}
