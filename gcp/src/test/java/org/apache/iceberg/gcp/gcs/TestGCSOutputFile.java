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
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.cloud.WriteChannel;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageOutputStream;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class TestGCSOutputFile {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String KEY = "file/path/a.dat";
  private static final String LOCATION = "gs://" + TEST_BUCKET + "/" + KEY;

  private Storage storage;
  private GcsFileSystem gcsFileSystem;
  private PrefixedStorage prefixedStorage;
  private GCPProperties gcpProperties;
  private MetricsContext metricsContext;
  private Blob blob;
  private BlobId blobId;

  @BeforeEach
  void before() {
    storage = mock(Storage.class);
    gcsFileSystem = mock(GcsFileSystem.class);
    prefixedStorage = mock(PrefixedStorage.class);
    gcpProperties = new GCPProperties();
    metricsContext = MetricsContext.nullMetrics();
    blob = mock(Blob.class);
    blobId = BlobId.fromGsUtilUri(LOCATION);

    WriteChannel writeChannel = mock(WriteChannel.class);
    when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption[].class)))
        .thenReturn(writeChannel);
  }

  @Test
  void fromLocationInitializesBlobIdAndLocation() {
    when(prefixedStorage.storage()).thenReturn(storage);
    when(prefixedStorage.gcsFileSystem()).thenReturn(gcsFileSystem);
    when(prefixedStorage.gcpProperties()).thenReturn(gcpProperties);

    GCSOutputFile outputFile =
        GCSOutputFile.fromLocation(LOCATION, prefixedStorage, metricsContext);

    assertThat(outputFile.blobId()).isEqualTo(blobId);
    assertThat(outputFile.location()).isEqualTo(LOCATION);
  }

  @Test
  void createWhenExistsThrowsAlreadyExistsException() {
    when(storage.get(blobId)).thenReturn(blob);

    GCSOutputFile outputFile =
        new GCSOutputFile(storage, gcsFileSystem, blobId, gcpProperties, metricsContext);

    assertThat(outputFile.exists()).isTrue();
    assertThatThrownBy(outputFile::create)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Location already exists: " + LOCATION);
  }

  @Test
  void createWhenDoesNotExistSucceeds() throws IOException {
    when(storage.get(blobId)).thenReturn(null);

    GCSOutputFile outputFile =
        new GCSOutputFile(storage, gcsFileSystem, blobId, gcpProperties, metricsContext);

    assertThat(outputFile.exists()).isFalse();
    try (PositionOutputStream stream = outputFile.create()) {
      assertThat(stream).isNotNull();
    }
  }

  @Test
  void createOrOverwriteWhenAnalyticsCoreEnabledReturnsAnalyticsCoreStream() throws IOException {
    GCPProperties enabledProperties =
        new GCPProperties(ImmutableMap.of(GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "true"));
    GoogleCloudStorageOutputStream mockStream = mock(GoogleCloudStorageOutputStream.class);
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(KEY).build();
    try (MockedStatic<GoogleCloudStorageOutputStream> mocked =
        mockStatic(GoogleCloudStorageOutputStream.class)) {
      mocked
          .when(
              () ->
                  GoogleCloudStorageOutputStream.create(
                      eq(gcsFileSystem), eq(expectedItemId), any(GcsWriteOptions.class)))
          .thenReturn(mockStream);
      GCSOutputFile outputFile =
          new GCSOutputFile(storage, gcsFileSystem, blobId, enabledProperties, metricsContext);

      try (PositionOutputStream stream = outputFile.createOrOverwrite()) {
        assertThat(stream).isNotInstanceOf(GCSOutputStream.class);
        assertThat(stream).isInstanceOf(AnalyticsCoreUtil.GcsOutputStreamWrapper.class);
      }
      mocked.verify(
          () ->
              GoogleCloudStorageOutputStream.create(
                  eq(gcsFileSystem), eq(expectedItemId), any(GcsWriteOptions.class)));
    }
  }

  @Test
  void createOrOverwriteWhenAnalyticsCoreDisabledReturnsGcsOutputStream() throws IOException {
    GCPProperties disabledProperties =
        new GCPProperties(ImmutableMap.of(GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "false"));
    GCSOutputFile outputFile =
        new GCSOutputFile(storage, gcsFileSystem, blobId, disabledProperties, metricsContext);

    try (MockedConstruction<GCSOutputStream> mocked =
        mockConstruction(
            GCSOutputStream.class,
            (mock, context) -> {
              assertThat(context.arguments()).hasSize(4);
              assertThat(context.arguments().get(0)).isEqualTo(storage);
              assertThat(context.arguments().get(1)).isEqualTo(blobId);
              assertThat(context.arguments().get(2)).isEqualTo(disabledProperties);
              assertThat(context.arguments().get(3)).isEqualTo(metricsContext);
            })) {
      try (PositionOutputStream stream = outputFile.createOrOverwrite()) {
        assertThat(stream).isNotInstanceOf(AnalyticsCoreUtil.GcsOutputStreamWrapper.class);
        assertThat(stream).isInstanceOf(GCSOutputStream.class);
        assertThat(mocked.constructed()).hasSize(1);
      }
    }
  }

  @Test
  void createOrOverwriteFallsBackToGcsOutputStreamWhenAnalyticsCoreFails() throws IOException {
    GCPProperties enabledGcpProperties =
        new GCPProperties(ImmutableMap.of(GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "true"));
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(KEY).build();

    try (MockedStatic<GoogleCloudStorageOutputStream> mocked =
        mockStatic(GoogleCloudStorageOutputStream.class)) {
      mocked
          .when(
              () ->
                  GoogleCloudStorageOutputStream.create(
                      eq(gcsFileSystem), eq(expectedItemId), any(GcsWriteOptions.class)))
          .thenThrow(new IOException("Analytics Core initialization failed"));

      GCSOutputFile outputFile =
          new GCSOutputFile(storage, gcsFileSystem, blobId, enabledGcpProperties, metricsContext);

      try (MockedConstruction<GCSOutputStream> outputStreamMocked =
          mockConstruction(
              GCSOutputStream.class,
              (mock, context) -> {
                assertThat(context.arguments()).hasSize(4);
                assertThat(context.arguments().get(0)).isEqualTo(storage);
                assertThat(context.arguments().get(1)).isEqualTo(blobId);
                assertThat(context.arguments().get(2)).isEqualTo(enabledGcpProperties);
                assertThat(context.arguments().get(3)).isEqualTo(metricsContext);
              })) {
        PositionOutputStream stream = outputFile.createOrOverwrite();
        assertThat(stream).isInstanceOf(GCSOutputStream.class);
        assertThat(outputStreamMocked.constructed()).hasSize(1);
        mocked.verify(
            () ->
                GoogleCloudStorageOutputStream.create(
                    eq(gcsFileSystem), eq(expectedItemId), any(GcsWriteOptions.class)));
        stream.close();
      }
    }
  }

  @Test
  void toInputFileReturnsGcsInputFileWithSameLocation() {
    GCSOutputFile outputFile =
        new GCSOutputFile(storage, gcsFileSystem, blobId, gcpProperties, metricsContext);
    InputFile inputFile = outputFile.toInputFile();
    assertThat(inputFile).isInstanceOf(GCSInputFile.class);
    assertThat(inputFile.location()).isEqualTo(LOCATION);
  }
}
