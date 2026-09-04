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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.util.PropertyUtil;

public class GCPProperties implements Serializable {
  // Service Options
  public static final String GCS_PROJECT_ID = "gcs.project-id";
  public static final String GCS_CLIENT_LIB_TOKEN = "gcs.client-lib-token";
  public static final String GCS_SERVICE_HOST = "gcs.service.host";

  // GCS Configuration Properties
  public static final String GCS_DECRYPTION_KEY = "gcs.decryption-key";
  public static final String GCS_ENCRYPTION_KEY = "gcs.encryption-key";
  public static final String GCS_USER_PROJECT = "gcs.user-project";

  private static final int MB = 1024 * 1024;

  public static final String GCS_CHANNEL_READ_CHUNK_SIZE = "gcs.channel.read.chunk-size-bytes";
  public static final String GCS_CHANNEL_WRITE_CHUNK_SIZE = "gcs.channel.write.chunk-size-bytes";
  public static final int GCS_CHANNEL_WRITE_CHUNK_SIZE_DEFAULT = 24 * MB;

  // KMS Encryption Options
  public static final String GCS_KMS_KEY_NAME = "gcs.kms-key-name";

  // Checksum Validation
  public static final String GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_ENABLED =
      "gcs.channel.write.checksum-validation.enabled";
  public static final boolean GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_DEFAULT = false;

  /**
   * Upload strategy used by GCS write channels when {@code gcs-analytics-core} is enabled (matches
   * {@code com.google.cloud.gcs.analyticscore.client.GcsClientOptions.UploadType}).
   *
   * <p>Supported values are:
   *
   * <ul>
   *   <li>{@link #GCS_CHANNEL_WRITE_UPLOAD_TYPE_CHUNK_UPLOAD} (default): Direct streaming chunked
   *       upload.
   *   <li>{@link #GCS_CHANNEL_WRITE_UPLOAD_TYPE_PARALLEL_COMPOSITE_UPLOAD}: Parallel composite
   *       upload (PCU) splitting large files across multiple concurrent threads.
   *   <li>{@link #GCS_CHANNEL_WRITE_UPLOAD_TYPE_WRITE_TO_DISK_THEN_UPLOAD}: Buffers data to local
   *       disk before uploading.
   * </ul>
   */
  public static final String GCS_CHANNEL_WRITE_UPLOAD_TYPE = "gcs.channel.write.upload-type";

  public static final String GCS_CHANNEL_WRITE_UPLOAD_TYPE_CHUNK_UPLOAD = "CHUNK_UPLOAD";
  public static final String GCS_CHANNEL_WRITE_UPLOAD_TYPE_PARALLEL_COMPOSITE_UPLOAD =
      "PARALLEL_COMPOSITE_UPLOAD";
  public static final String GCS_CHANNEL_WRITE_UPLOAD_TYPE_WRITE_TO_DISK_THEN_UPLOAD =
      "WRITE_TO_DISK_THEN_UPLOAD";
  public static final String GCS_CHANNEL_WRITE_UPLOAD_TYPE_DEFAULT =
      GCS_CHANNEL_WRITE_UPLOAD_TYPE_CHUNK_UPLOAD;

  // Parallel Composite Upload (PCU) Options (matches GcsClientOptions PCU keys)
  public static final String GCS_CHANNEL_WRITE_PCU_BUFFER_COUNT =
      "gcs.channel.write.pcu.buffer.count";
  public static final int GCS_CHANNEL_WRITE_PCU_BUFFER_COUNT_DEFAULT = 1;

  public static final String GCS_CHANNEL_WRITE_PCU_BUFFER_CAPACITY =
      "gcs.channel.write.pcu.buffer.capacity-bytes";
  public static final int GCS_CHANNEL_WRITE_PCU_BUFFER_CAPACITY_DEFAULT = 32 * MB;

  /**
   * Cleanup policy for temporary part files generated during parallel composite uploads.
   *
   * <p>Supported values are:
   *
   * <ul>
   *   <li>{@link #GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_ALWAYS} (default): Deletes part files on both
   *       success and failure.
   *   <li>{@link #GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_NEVER}: Retains part files indefinitely.
   *   <li>{@link #GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_ON_SUCCESS}: Deletes part files only upon
   *       successful upload.
   * </ul>
   */
  public static final String GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE =
      "gcs.channel.write.pcu.part-file.cleanup-type";

  public static final String GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_ALWAYS = "ALWAYS";
  public static final String GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_NEVER = "NEVER";
  public static final String GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_ON_SUCCESS = "ON_SUCCESS";
  public static final String GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_DEFAULT =
      GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_ALWAYS;

  public static final String GCS_CHANNEL_WRITE_PCU_NAME_PREFIX =
      "gcs.channel.write.pcu.part-file.name-prefix";
  public static final String GCS_CHANNEL_WRITE_PCU_NAME_PREFIX_DEFAULT = "";

  public static final String GCS_CHANNEL_WRITE_TEMPORARY_PATHS =
      "gcs.channel.write.temporary-paths";

  public static final String GCS_OAUTH2_TOKEN = "gcs.oauth2.token";
  public static final String GCS_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";
  // Boolean to explicitly configure "no authentication" for testing purposes using a GCS emulator
  public static final String GCS_NO_AUTH = "gcs.no-auth";
  public static final String GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT =
      "gcs.oauth2.refresh-credentials-endpoint";

  // Impersonation properties
  public static final String GCS_IMPERSONATE_SERVICE_ACCOUNT = "gcs.impersonate.service-account";
  public static final String GCS_IMPERSONATE_LIFETIME_SECONDS = "gcs.impersonate.lifetime-seconds";
  public static final String GCS_IMPERSONATE_DELEGATES = "gcs.impersonate.delegates";
  public static final String GCS_IMPERSONATE_SCOPES = "gcs.impersonate.scopes";
  public static final int GCS_IMPERSONATE_LIFETIME_SECONDS_DEFAULT = 3600;
  private static final List<String> GCS_IMPERSONATE_SCOPES_DEFAULT =
      ImmutableList.of("https://www.googleapis.com/auth/cloud-platform");

  /** Controls whether vended credentials should be refreshed or not. Defaults to true. */
  public static final String GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED =
      "gcs.oauth2.refresh-credentials-enabled";

  /** Configure the batch size used when deleting multiple files from a given GCS bucket */
  public static final String GCS_DELETE_BATCH_SIZE = "gcs.delete.batch-size";

  /** Controls whether analytics core library is enabled or not. Defaults to false. */
  public static final String GCS_ANALYTICS_CORE_ENABLED = "gcs.analytics-core.enabled";

  /**
   * Max possible batch size for deletion. Currently, a max of 100 keys is advised, so we default to
   * a number below that. https://cloud.google.com/storage/docs/batch
   */
  public static final int GCS_DELETE_BATCH_SIZE_DEFAULT = 50;

  private final Map<String, String> allProperties;

  private String projectId;
  private String clientLibToken;
  private String serviceHost;

  private String gcsDecryptionKey;
  private String gcsEncryptionKey;
  private String gcsKmsKeyName;
  private String gcsUserProject;

  private Integer gcsChannelReadChunkSize;
  private Integer gcsChannelWriteChunkSize;

  private boolean gcsNoAuth;
  private String gcsOAuth2Token;
  private Date gcsOAuth2TokenExpiresAt;
  private String gcsOauth2RefreshCredentialsEndpoint;
  private boolean gcsOauth2RefreshCredentialsEnabled;
  private boolean gcsAnalyticsCoreEnabled;

  private boolean gcsChecksumValidationEnabled = GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_DEFAULT;
  private String gcsChannelWriteUploadType = GCS_CHANNEL_WRITE_UPLOAD_TYPE_DEFAULT;
  private int gcsChannelWritePcuBufferCount = GCS_CHANNEL_WRITE_PCU_BUFFER_COUNT_DEFAULT;
  private int gcsChannelWritePcuBufferCapacity = GCS_CHANNEL_WRITE_PCU_BUFFER_CAPACITY_DEFAULT;
  private String gcsChannelWritePcuCleanupType = GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_DEFAULT;
  private String gcsChannelWritePcuNamePrefix = GCS_CHANNEL_WRITE_PCU_NAME_PREFIX_DEFAULT;
  private List<String> gcsChannelWriteTemporaryPaths;

  private String gcsImpersonateServiceAccount;
  private int gcsImpersonateLifetimeSeconds;
  private List<String> gcsImpersonateDelegates;
  private List<String> gcsImpersonateScopes;

  private int gcsDeleteBatchSize = GCS_DELETE_BATCH_SIZE_DEFAULT;

  @VisibleForTesting
  List<String> parseCommaSeparatedList(String input, List<String> defaultValue) {
    if (input == null || input.trim().isEmpty()) {
      return defaultValue;
    }
    return Arrays.stream(input.split(","))
        .map(String::trim)
        .filter(str -> !str.isEmpty())
        .distinct()
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  List<String> expandScopes(List<String> inputScopes) {
    if (inputScopes == null || inputScopes.isEmpty()) {
      return inputScopes;
    }
    return inputScopes.stream()
        .map(
            inputScope -> {
              if (inputScope.startsWith("https://")) {
                return inputScope;
              }
              if (inputScope.startsWith("http://")) {
                return inputScope.replace("http://", "https://");
              }

              return "https://www.googleapis.com/auth/" + inputScope;
            })
        .collect(Collectors.toList());
  }

  public GCPProperties() {
    this.allProperties = ImmutableMap.of();
  }

  @SuppressWarnings("JavaUtilDate") // GCP API uses java.util.Date
  public GCPProperties(Map<String, String> properties) {
    this.allProperties = ImmutableMap.copyOf(properties);
    projectId = properties.get(GCS_PROJECT_ID);
    clientLibToken = properties.get(GCS_CLIENT_LIB_TOKEN);
    serviceHost = properties.get(GCS_SERVICE_HOST);

    gcsDecryptionKey = properties.get(GCS_DECRYPTION_KEY);
    gcsEncryptionKey = properties.get(GCS_ENCRYPTION_KEY);
    gcsUserProject = properties.get(GCS_USER_PROJECT);

    if (properties.containsKey(GCS_CHANNEL_READ_CHUNK_SIZE)) {
      gcsChannelReadChunkSize = Integer.parseInt(properties.get(GCS_CHANNEL_READ_CHUNK_SIZE));
    }

    if (properties.containsKey(GCS_CHANNEL_WRITE_CHUNK_SIZE)) {
      gcsChannelWriteChunkSize = Integer.parseInt(properties.get(GCS_CHANNEL_WRITE_CHUNK_SIZE));
    }

    gcsOAuth2Token = properties.get(GCS_OAUTH2_TOKEN);
    if (properties.containsKey(GCS_OAUTH2_TOKEN_EXPIRES_AT)) {
      gcsOAuth2TokenExpiresAt =
          new Date(Long.parseLong(properties.get(GCS_OAUTH2_TOKEN_EXPIRES_AT)));
    }

    gcsOauth2RefreshCredentialsEndpoint =
        RESTUtil.resolveEndpoint(
            properties.get(CatalogProperties.URI),
            properties.get(GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT));
    gcsOauth2RefreshCredentialsEnabled =
        PropertyUtil.propertyAsBoolean(properties, GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, true);
    gcsNoAuth = Boolean.parseBoolean(properties.getOrDefault(GCS_NO_AUTH, "false"));
    Preconditions.checkState(
        !(gcsOAuth2Token != null && gcsNoAuth),
        "Invalid auth settings: must not configure %s and %s",
        GCS_NO_AUTH,
        GCS_OAUTH2_TOKEN);

    gcsDeleteBatchSize =
        PropertyUtil.propertyAsInt(
            properties, GCS_DELETE_BATCH_SIZE, GCS_DELETE_BATCH_SIZE_DEFAULT);

    gcsImpersonateServiceAccount = properties.get(GCS_IMPERSONATE_SERVICE_ACCOUNT);
    gcsImpersonateLifetimeSeconds =
        PropertyUtil.propertyAsInt(
            properties, GCS_IMPERSONATE_LIFETIME_SECONDS, GCS_IMPERSONATE_LIFETIME_SECONDS_DEFAULT);
    gcsImpersonateDelegates =
        parseCommaSeparatedList(properties.get(GCS_IMPERSONATE_DELEGATES), null);
    List<String> rawScopes =
        parseCommaSeparatedList(
            properties.get(GCS_IMPERSONATE_SCOPES), GCS_IMPERSONATE_SCOPES_DEFAULT);
    gcsImpersonateScopes = expandScopes(rawScopes);

    gcsAnalyticsCoreEnabled =
        PropertyUtil.propertyAsBoolean(properties, GCS_ANALYTICS_CORE_ENABLED, false);

    this.gcsKmsKeyName = properties.get(GCS_KMS_KEY_NAME);
    this.gcsChecksumValidationEnabled =
        PropertyUtil.propertyAsBoolean(
            properties,
            GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_ENABLED,
            GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_DEFAULT);

    this.gcsChannelWriteUploadType =
        properties.getOrDefault(
            GCS_CHANNEL_WRITE_UPLOAD_TYPE, GCS_CHANNEL_WRITE_UPLOAD_TYPE_DEFAULT);

    this.gcsChannelWritePcuBufferCount =
        PropertyUtil.propertyAsInt(
            properties,
            GCS_CHANNEL_WRITE_PCU_BUFFER_COUNT,
            GCS_CHANNEL_WRITE_PCU_BUFFER_COUNT_DEFAULT);

    this.gcsChannelWritePcuBufferCapacity =
        PropertyUtil.propertyAsInt(
            properties,
            GCS_CHANNEL_WRITE_PCU_BUFFER_CAPACITY,
            GCS_CHANNEL_WRITE_PCU_BUFFER_CAPACITY_DEFAULT);

    this.gcsChannelWritePcuCleanupType =
        properties.getOrDefault(
            GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE, GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE_DEFAULT);

    this.gcsChannelWritePcuNamePrefix =
        properties.getOrDefault(
            GCS_CHANNEL_WRITE_PCU_NAME_PREFIX, GCS_CHANNEL_WRITE_PCU_NAME_PREFIX_DEFAULT);

    List<String> tempPaths =
        parseCommaSeparatedList(properties.get(GCS_CHANNEL_WRITE_TEMPORARY_PATHS), null);
    this.gcsChannelWriteTemporaryPaths = tempPaths != null ? ImmutableList.copyOf(tempPaths) : null;
  }

  public Optional<Integer> channelReadChunkSize() {
    return Optional.ofNullable(gcsChannelReadChunkSize);
  }

  public Optional<Integer> channelWriteChunkSize() {
    return Optional.ofNullable(gcsChannelWriteChunkSize);
  }

  public Optional<String> clientLibToken() {
    return Optional.ofNullable(clientLibToken);
  }

  public Optional<String> decryptionKey() {
    return Optional.ofNullable(gcsDecryptionKey);
  }

  public Optional<String> encryptionKey() {
    return Optional.ofNullable(gcsEncryptionKey);
  }

  public Optional<String> projectId() {
    return Optional.ofNullable(projectId);
  }

  public Optional<String> serviceHost() {
    return Optional.ofNullable(serviceHost);
  }

  public Optional<String> userProject() {
    return Optional.ofNullable(gcsUserProject);
  }

  public Optional<String> oauth2Token() {
    return Optional.ofNullable(gcsOAuth2Token);
  }

  public boolean noAuth() {
    return gcsNoAuth;
  }

  public Optional<Date> oauth2TokenExpiresAt() {
    return Optional.ofNullable(gcsOAuth2TokenExpiresAt);
  }

  public Optional<String> impersonateServiceAccount() {
    return Optional.ofNullable(gcsImpersonateServiceAccount);
  }

  public int impersonateLifetimeSeconds() {
    return gcsImpersonateLifetimeSeconds;
  }

  public List<String> impersonateDelegates() {
    return gcsImpersonateDelegates;
  }

  public List<String> impersonateScopes() {
    return gcsImpersonateScopes;
  }

  public int deleteBatchSize() {
    return gcsDeleteBatchSize;
  }

  public Optional<String> oauth2RefreshCredentialsEndpoint() {
    return Optional.ofNullable(gcsOauth2RefreshCredentialsEndpoint);
  }

  public boolean oauth2RefreshCredentialsEnabled() {
    return gcsOauth2RefreshCredentialsEnabled;
  }

  public Map<String, String> properties() {
    return allProperties;
  }

  public boolean isGcsAnalyticsCoreEnabled() {
    return gcsAnalyticsCoreEnabled;
  }

  public String kmsKeyName() {
    return gcsKmsKeyName;
  }

  public boolean checksumValidationEnabled() {
    return gcsChecksumValidationEnabled;
  }

  public String channelWriteUploadType() {
    return gcsChannelWriteUploadType;
  }

  public int channelWritePcuBufferCount() {
    return gcsChannelWritePcuBufferCount;
  }

  public int channelWritePcuBufferCapacity() {
    return gcsChannelWritePcuBufferCapacity;
  }

  public String channelWritePcuCleanupType() {
    return gcsChannelWritePcuCleanupType;
  }

  public String channelWritePcuNamePrefix() {
    return gcsChannelWritePcuNamePrefix;
  }

  public List<String> channelWriteTemporaryPaths() {
    return gcsChannelWriteTemporaryPaths;
  }
}
