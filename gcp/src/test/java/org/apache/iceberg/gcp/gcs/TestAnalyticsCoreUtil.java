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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageOutputStream;
import com.google.cloud.storage.BlobId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.CachingMetricsContext;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class TestAnalyticsCoreUtil {

  @Test
  public void readVectored() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream gcsInputStream = mock(GoogleCloudStorageInputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");

    SeekableInputStream stream;
    try (MockedStatic<GoogleCloudStorageInputStream> mocked =
        mockStatic(GoogleCloudStorageInputStream.class)) {
      mocked
          .when(() -> GoogleCloudStorageInputStream.create(eq(fileSystem), any(GcsItemId.class)))
          .thenReturn(gcsInputStream);
      stream = AnalyticsCoreUtil.newStream(fileSystem, blobId, null, MetricsContext.nullMetrics());
    }

    CompletableFuture<ByteBuffer> future1 = new CompletableFuture<>();
    CompletableFuture<ByteBuffer> future2 = new CompletableFuture<>();
    List<FileRange> ranges =
        List.of(new FileRange(future1, 10L, 100), new FileRange(future2, 0, 50));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    ((RangeReadable) stream).readVectored(ranges, allocate);

    List<GcsObjectRange> objectRanges =
        List.of(
            GcsObjectRange.builder()
                .setOffset(10)
                .setLength(100)
                .setByteBufferFuture(future1)
                .build(),
            GcsObjectRange.builder()
                .setOffset(0)
                .setLength(50)
                .setByteBufferFuture(future2)
                .build());
    verify(gcsInputStream).readVectored(objectRanges, allocate);
  }

  @Test
  public void readVectoredCountsRequestedLengthSynchronously() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream gcsInputStream = mock(GoogleCloudStorageInputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    SeekableInputStream stream;
    try (MockedStatic<GoogleCloudStorageInputStream> mocked =
        mockStatic(GoogleCloudStorageInputStream.class)) {
      mocked
          .when(() -> GoogleCloudStorageInputStream.create(eq(fileSystem), any(GcsItemId.class)))
          .thenReturn(gcsInputStream);
      stream = AnalyticsCoreUtil.newStream(fileSystem, blobId, null, metrics);
    }

    CompletableFuture<ByteBuffer> future1 = new CompletableFuture<>();
    CompletableFuture<ByteBuffer> future2 = new CompletableFuture<>();
    List<FileRange> ranges =
        List.of(new FileRange(future1, 10L, 100), new FileRange(future2, 200L, 50));

    ((RangeReadable) stream).readVectored(ranges, ByteBuffer::allocate);

    // Bytes are counted synchronously on the caller thread, using the requested range length,
    // before the range futures complete. This is deliberate: the futures complete on analytics-core
    // background threads, and counting there would attribute bytes to the wrong thread's Hadoop
    // Statistics and never reach Spark's task input metrics (see AnalyticsCoreUtil#readVectored).
    // The tradeoff is a bounded over-count on short reads / failed ranges.
    //
    // NOTE: this test uses a plain LongAdder-backed counter, which cannot verify the per-thread
    // Statistics attribution that motivates counting synchronously; that correctness argument
    // rests on the HadoopMetricsContext reasoning, not on this assertion.
    assertThat(readBytes.value()).isEqualTo(150);
    assertThat(readOperations.value()).isEqualTo(2);

    // completing (or failing) the futures afterward does not change the already-recorded metrics
    future1.complete(ByteBuffer.allocate(100));
    future2.completeExceptionally(new IOException("boom"));
    assertThat(readBytes.value()).isEqualTo(150);
    assertThat(readOperations.value()).isEqualTo(2);
  }

  @Test
  public void readDoesNotCountAtEof() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream gcsInputStream = mock(GoogleCloudStorageInputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");
    // a real byte, then EOF; a buffered read of 8 bytes, then EOF
    when(gcsInputStream.read()).thenReturn(42).thenReturn(-1);
    when(gcsInputStream.read(any(byte[].class), anyInt(), anyInt())).thenReturn(8).thenReturn(-1);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    SeekableInputStream stream;
    try (MockedStatic<GoogleCloudStorageInputStream> mocked =
        mockStatic(GoogleCloudStorageInputStream.class)) {
      mocked
          .when(() -> GoogleCloudStorageInputStream.create(eq(fileSystem), any(GcsItemId.class)))
          .thenReturn(gcsInputStream);
      stream = AnalyticsCoreUtil.newStream(fileSystem, blobId, null, metrics);
    }

    assertThat(stream.read()).isEqualTo(42);
    assertThat(readBytes.value()).isEqualTo(1);
    assertThat(readOperations.value()).isEqualTo(1);

    assertThat(stream.read(new byte[16], 0, 16)).isEqualTo(8);
    assertThat(readBytes.value()).isEqualTo(9);
    assertThat(readOperations.value()).isEqualTo(2);

    // EOF reads count neither bytes nor an operation
    assertThat(stream.read()).isEqualTo(-1);
    assertThat(stream.read(new byte[16], 0, 16)).isEqualTo(-1);
    assertThat(readBytes.value()).isEqualTo(9);
    assertThat(readOperations.value()).isEqualTo(2);
  }

  @Test
  public void readVectoredSkipsZeroLengthRanges() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream gcsInputStream = mock(GoogleCloudStorageInputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    SeekableInputStream stream;
    try (MockedStatic<GoogleCloudStorageInputStream> mocked =
        mockStatic(GoogleCloudStorageInputStream.class)) {
      mocked
          .when(() -> GoogleCloudStorageInputStream.create(eq(fileSystem), any(GcsItemId.class)))
          .thenReturn(gcsInputStream);
      stream = AnalyticsCoreUtil.newStream(fileSystem, blobId, null, metrics);
    }

    CompletableFuture<ByteBuffer> future1 = new CompletableFuture<>();
    CompletableFuture<ByteBuffer> future2 = new CompletableFuture<>();
    // a zero-length range must not count bytes or an operation
    List<FileRange> ranges =
        List.of(new FileRange(future1, 10L, 100), new FileRange(future2, 200L, 0));

    ((RangeReadable) stream).readVectored(ranges, ByteBuffer::allocate);

    assertThat(readBytes.value()).isEqualTo(100);
    assertThat(readOperations.value()).isEqualTo(1);
  }

  @Test
  public void readTailDoesNotCountAtEof() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GoogleCloudStorageInputStream gcsInputStream = mock(GoogleCloudStorageInputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");
    // a tail read of 8 bytes, then an empty/EOF tail read
    when(gcsInputStream.readTail(any(byte[].class), anyInt(), anyInt()))
        .thenReturn(8)
        .thenReturn(-1);

    CachingMetricsContext metrics = new CachingMetricsContext();
    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    SeekableInputStream stream;
    try (MockedStatic<GoogleCloudStorageInputStream> mocked =
        mockStatic(GoogleCloudStorageInputStream.class)) {
      mocked
          .when(() -> GoogleCloudStorageInputStream.create(eq(fileSystem), any(GcsItemId.class)))
          .thenReturn(gcsInputStream);
      stream = AnalyticsCoreUtil.newStream(fileSystem, blobId, null, metrics);
    }

    assertThat(((RangeReadable) stream).readTail(new byte[16], 0, 16)).isEqualTo(8);
    assertThat(readBytes.value()).isEqualTo(8);
    assertThat(readOperations.value()).isEqualTo(1);

    // an empty/EOF tail read counts neither bytes nor an operation
    assertThat(((RangeReadable) stream).readTail(new byte[16], 0, 16)).isEqualTo(-1);
    assertThat(readBytes.value()).isEqualTo(8);
    assertThat(readOperations.value()).isEqualTo(1);
  }

  @Test
  void writeOptionsConfiguration() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName("mockbucket").setObjectName("mockname").build();

    GCPProperties properties =
        new GCPProperties(
            ImmutableMap.<String, String>builder()
                .put(
                    GCPProperties.GCS_KMS_KEY_NAME,
                    "projects/p/locations/l/keyRings/r/cryptoKeys/k")
                .put(GCPProperties.GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_ENABLED, "true")
                .put(GCPProperties.GCS_USER_PROJECT, "my-user-project")
                .build());

    GoogleCloudStorageOutputStream mockStream = mock(GoogleCloudStorageOutputStream.class);
    ArgumentCaptor<GcsWriteOptions> optionsCaptor = ArgumentCaptor.forClass(GcsWriteOptions.class);

    try (MockedStatic<GoogleCloudStorageOutputStream> mocked =
        mockStatic(GoogleCloudStorageOutputStream.class)) {
      mocked
          .when(
              () ->
                  GoogleCloudStorageOutputStream.create(
                      eq(fileSystem), eq(expectedItemId), optionsCaptor.capture()))
          .thenReturn(mockStream);

      PositionOutputStream outputStream =
          AnalyticsCoreUtil.newOutputStream(
              fileSystem, blobId, properties, MetricsContext.nullMetrics());

      assertThat(outputStream).isNotNull();
      GcsWriteOptions captured = optionsCaptor.getValue();
      assertThat(captured.isOverwriteExisting()).isTrue();
      assertThat(captured.isDisableGzipContent()).isTrue();
      assertThat(captured.isChecksumValidationEnabled()).isTrue();
      assertThat(captured.getKmsKeyName())
          .contains("projects/p/locations/l/keyRings/r/cryptoKeys/k");
      assertThat(captured.getUserProject()).contains("my-user-project");
      assertThat(captured.getEncryptionKey()).isEmpty();
    }

    // Also verify CSEK configuration
    GCPProperties csekProperties =
        new GCPProperties(
            ImmutableMap.of(
                GCPProperties.GCS_ENCRYPTION_KEY,
                "csek-key",
                GCPProperties.GCS_CHANNEL_WRITE_CHECKSUM_VALIDATION_ENABLED,
                "false"));

    try (MockedStatic<GoogleCloudStorageOutputStream> mocked =
        mockStatic(GoogleCloudStorageOutputStream.class)) {
      mocked
          .when(
              () ->
                  GoogleCloudStorageOutputStream.create(
                      eq(fileSystem), eq(expectedItemId), optionsCaptor.capture()))
          .thenReturn(mockStream);

      PositionOutputStream outputStream =
          AnalyticsCoreUtil.newOutputStream(
              fileSystem, blobId, csekProperties, MetricsContext.nullMetrics());

      assertThat(outputStream).isNotNull();
      GcsWriteOptions captured = optionsCaptor.getValue();
      assertThat(captured.isOverwriteExisting()).isTrue();
      assertThat(captured.isDisableGzipContent()).isTrue();
      assertThat(captured.isChecksumValidationEnabled()).isFalse();
      assertThat(captured.getEncryptionKey()).contains("csek-key");
      assertThat(captured.getKmsKeyName()).isEmpty();
    }
  }

  @Test
  void streamPositionAndMetricsTracking() throws IOException {
    GoogleCloudStorageOutputStream mockStream = mock(GoogleCloudStorageOutputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");

    MetricsContext metrics = mock(MetricsContext.class);
    Counter writeBytes = mock(Counter.class);
    Counter writeOperations = mock(Counter.class);
    when(metrics.counter(FileIOMetricsContext.WRITE_BYTES, MetricsContext.Unit.BYTES))
        .thenReturn(writeBytes);
    when(metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS)).thenReturn(writeOperations);

    PositionOutputStream stream =
        new AnalyticsCoreUtil.GcsOutputStreamWrapper(mockStream, blobId, metrics);

    assertThat(stream.getPos()).isEqualTo(0L);
    assertThat(stream.storedLength()).isEqualTo(0L);

    stream.write(42);
    assertThat(stream.getPos()).isEqualTo(1L);
    assertThat(stream.storedLength()).isEqualTo(1L);
    verify(mockStream).write(42);
    verify(writeBytes).increment();
    verify(writeOperations, times(1)).increment();

    byte[] bytes = new byte[] {1, 2, 3, 4, 5};
    stream.write(bytes, 1, 3);
    assertThat(stream.getPos()).isEqualTo(4L);
    verify(mockStream).write(bytes, 1, 3);
    verify(writeBytes).increment(3);
    verify(writeOperations, times(2)).increment();

    stream.write(new byte[] {6, 7});
    assertThat(stream.getPos()).isEqualTo(6L);
    verify(mockStream).write(new byte[] {6, 7}, 0, 2);
    verify(writeBytes).increment(2);
    verify(writeOperations, times(3)).increment();

    stream.close();
    verify(mockStream).close();

    // Verify close is idempotent
    stream.close();
    verify(mockStream, times(1)).close();
  }

  @SuppressWarnings({"deprecation", "checkstyle:NoFinalizer", "Finalize"})
  @Test
  void leakDetectionStackTraceLogged() throws Exception {
    GoogleCloudStorageOutputStream mockStream = mock(GoogleCloudStorageOutputStream.class);
    BlobId blobId = BlobId.of("mockbucket", "mockname");

    AnalyticsCoreUtil.GcsOutputStreamWrapper stream =
        new AnalyticsCoreUtil.GcsOutputStreamWrapper(
            mockStream, blobId, MetricsContext.nullMetrics());

    // When finalize is called on unclosed stream, it should close the stream
    try {
      stream.finalize();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    verify(mockStream).close();

    // Calling close again should be a no-op since it was closed by finalizer
    stream.close();
    verify(mockStream, times(1)).close();

    // Verify finalize handles exception during close without throwing
    GoogleCloudStorageOutputStream failingStream = mock(GoogleCloudStorageOutputStream.class);
    doThrow(new IOException("close failed")).when(failingStream).close();
    AnalyticsCoreUtil.GcsOutputStreamWrapper failingWrapper =
        new AnalyticsCoreUtil.GcsOutputStreamWrapper(
            failingStream, blobId, MetricsContext.nullMetrics());

    assertThatCode(
            () -> {
              try {
                failingWrapper.finalize();
              } catch (Throwable t) {
                throw new RuntimeException(t);
              }
            })
        .doesNotThrowAnyException();
    verify(failingStream).close();
  }

  @Test
  void enumNormalizationAndGuardrailValidation() {
    assertThat(AnalyticsCoreUtil.parseUploadType("chunk-upload"))
        .isEqualTo(GcsClientOptions.UploadType.CHUNK_UPLOAD);
    assertThat(AnalyticsCoreUtil.parseUploadType(" PARALLEL_COMPOSITE_UPLOAD "))
        .isEqualTo(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD);
    assertThat(AnalyticsCoreUtil.parseUploadType("write_to_disk_then_upload"))
        .isEqualTo(GcsClientOptions.UploadType.WRITE_TO_DISK_THEN_UPLOAD);

    assertThat(AnalyticsCoreUtil.parseCleanupType("always"))
        .isEqualTo(GcsClientOptions.PartFileCleanupType.ALWAYS);
    assertThat(AnalyticsCoreUtil.parseCleanupType(" on-success "))
        .isEqualTo(GcsClientOptions.PartFileCleanupType.ON_SUCCESS);
    assertThat(AnalyticsCoreUtil.parseCleanupType("NEVER"))
        .isEqualTo(GcsClientOptions.PartFileCleanupType.NEVER);

    assertThatThrownBy(() -> AnalyticsCoreUtil.parseUploadType("invalid-upload-type"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid upload type: 'invalid-upload-type'")
        .hasMessageContaining("CHUNK_UPLOAD");

    assertThatThrownBy(() -> AnalyticsCoreUtil.parseCleanupType("invalid-cleanup"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid part-file cleanup type: 'invalid-cleanup'")
        .hasMessageContaining("ALWAYS");
  }

  @Test
  void createFileSystemValidatesWriteProperties() {
    Map<String, String> invalidUploadType =
        ImmutableMap.of(
            GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "true",
            GCPProperties.GCS_CHANNEL_WRITE_UPLOAD_TYPE, "invalid-type");

    assertThatThrownBy(() -> AnalyticsCoreUtil.createFileSystem(invalidUploadType, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid upload type: 'invalid-type'")
        .hasMessageContaining("CHUNK_UPLOAD");

    Map<String, String> invalidCleanupType =
        ImmutableMap.of(
            GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "true",
            GCPProperties.GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE, "bad-cleanup");

    assertThatThrownBy(() -> AnalyticsCoreUtil.createFileSystem(invalidCleanupType, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid part-file cleanup type: 'bad-cleanup'")
        .hasMessageContaining("ALWAYS");
  }
}
