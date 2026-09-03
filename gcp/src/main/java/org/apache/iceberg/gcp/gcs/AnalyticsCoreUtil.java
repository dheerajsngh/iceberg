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

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.gcs.analyticscore.core.GcsAnalyticsCoreOptions;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageOutputStream;
import com.google.cloud.storage.BlobId;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gateway to the optional {@code com.google.cloud.gcs.analyticscore.*} dependency. All references
 * to analytics-core types are confined to this class so that it is loaded only when {@link
 * org.apache.iceberg.gcp.GCPProperties#GCS_ANALYTICS_CORE_ENABLED} is true.
 */
class AnalyticsCoreUtil {

  private AnalyticsCoreUtil() {}

  static AutoCloseable createFileSystem(Map<String, String> properties, Credentials credentials) {
    Preconditions.checkState(
        PropertyUtil.propertyAsBoolean(properties, GCPProperties.GCS_ANALYTICS_CORE_ENABLED, false),
        "GCS analytics-core is disabled; %s must be set to true",
        GCPProperties.GCS_ANALYTICS_CORE_ENABLED);

    String uploadType = properties.get(GCPProperties.GCS_CHANNEL_WRITE_UPLOAD_TYPE);
    if (uploadType != null) {
      parseUploadType(uploadType);
    }

    String cleanupType = properties.get(GCPProperties.GCS_CHANNEL_WRITE_PCU_CLEANUP_TYPE);
    if (cleanupType != null) {
      parseCleanupType(cleanupType);
    }

    GcsAnalyticsCoreOptions options = new GcsAnalyticsCoreOptions("gcs.", properties);
    GcsFileSystemOptions fileSystemOptions = options.getGcsFileSystemOptions();
    return credentials == null
        ? new GcsFileSystemImpl(fileSystemOptions)
        : new GcsFileSystemImpl(credentials, fileSystemOptions);
  }

  static SeekableInputStream newStream(
      AutoCloseable fileSystemHandle, BlobId blobId, Long blobSize, MetricsContext metrics)
      throws IOException {
    GcsFileSystem fileSystem = (GcsFileSystem) fileSystemHandle;
    GcsItemId itemId = gcsItemId(blobId);
    GoogleCloudStorageInputStream stream =
        blobSize == null
            ? GoogleCloudStorageInputStream.create(fileSystem, itemId)
            : GoogleCloudStorageInputStream.create(
                fileSystem, gcsFileInfo(blobId, itemId, blobSize));
    return new GcsInputStreamWrapper(stream, blobId, metrics);
  }

  static PositionOutputStream newOutputStream(
      AutoCloseable fileSystemHandle,
      BlobId blobId,
      GCPProperties gcpProperties,
      MetricsContext metrics)
      throws IOException {
    GcsFileSystem fileSystem = (GcsFileSystem) fileSystemHandle;
    GcsItemId itemId = gcsItemId(blobId);

    GcsWriteOptions.Builder writeOptionsBuilder =
        GcsWriteOptions.builder()
            .setChecksumValidationEnabled(gcpProperties.checksumValidationEnabled());

    if (gcpProperties.kmsKeyName() != null) {
      writeOptionsBuilder.setKmsKeyName(gcpProperties.kmsKeyName());
    }
    gcpProperties.encryptionKey().ifPresent(writeOptionsBuilder::setEncryptionKey);
    gcpProperties.userProject().ifPresent(writeOptionsBuilder::setUserProject);

    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fileSystem, itemId, writeOptionsBuilder.build());

    return new GcsOutputStreamWrapper(stream, blobId, metrics);
  }

  private static <E extends Enum<E>> E parseEnum(
      String value, Class<E> enumClass, String propertyDescription) {
    Preconditions.checkArgument(value != null, "%s cannot be null", propertyDescription);
    String normalized = value.trim().replace('-', '_').toUpperCase(Locale.ROOT);
    try {
      return Enum.valueOf(enumClass, normalized);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s: '%s'. Expected one of: %s",
              propertyDescription, value, Arrays.toString(enumClass.getEnumConstants())));
    }
  }

  static GcsClientOptions.UploadType parseUploadType(String uploadType) {
    return parseEnum(uploadType, GcsClientOptions.UploadType.class, "upload type");
  }

  static GcsClientOptions.PartFileCleanupType parseCleanupType(String cleanupType) {
    return parseEnum(
        cleanupType, GcsClientOptions.PartFileCleanupType.class, "part-file cleanup type");
  }

  static void close(AutoCloseable fileSystemHandle) {
    if (fileSystemHandle != null) {
      ((GcsFileSystem) fileSystemHandle).close();
    }
  }

  private static GcsItemId gcsItemId(BlobId blobId) {
    GcsItemId.Builder builder =
        GcsItemId.builder().setBucketName(blobId.getBucket()).setObjectName(blobId.getName());
    if (blobId.getGeneration() != null) {
      builder.setContentGeneration(blobId.getGeneration());
    }

    return builder.build();
  }

  private static GcsFileInfo gcsFileInfo(BlobId blobId, GcsItemId itemId, long size) {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(size).build();
    return GcsFileInfo.builder()
        .setItemInfo(itemInfo)
        .setUri(URI.create(blobId.toGsUtilUri()))
        .setAttributes(ImmutableMap.of())
        .build();
  }

  private static class GcsInputStreamWrapper extends SeekableInputStream implements RangeReadable {
    private final Counter readBytes;
    private final Counter readOperations;
    private final GoogleCloudStorageInputStream stream;
    private final BlobId blobId;

    GcsInputStreamWrapper(
        GoogleCloudStorageInputStream stream, BlobId blobId, MetricsContext metrics) {
      Preconditions.checkArgument(null != stream, "Invalid input stream : null");
      Preconditions.checkArgument(null != blobId, "Invalid blobId : null");
      this.stream = stream;
      this.blobId = blobId;
      this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
      this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
    }

    @Override
    public long getPos() throws IOException {
      return stream.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      stream.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      int readByte;
      try {
        readByte = stream.read();
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
      if (readByte != -1) {
        readBytes.increment();
        readOperations.increment();
      }
      return readByte;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int bytesRead;
      try {
        bytesRead = stream.read(b, off, len);
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
      if (bytesRead > 0) {
        readBytes.increment(bytesRead);
        readOperations.increment();
      }
      return bytesRead;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      try {
        stream.readFully(position, buffer, offset, length);
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
      if (length > 0) {
        readBytes.increment(length);
        readOperations.increment();
      }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      int bytesRead;
      try {
        bytesRead = stream.readTail(buffer, offset, length);
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
      if (bytesRead > 0) {
        readBytes.increment(bytesRead);
        readOperations.increment();
      }
      return bytesRead;
    }

    @Override
    public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate)
        throws IOException {
      List<GcsObjectRange> objectRanges =
          ranges.stream()
              .map(
                  fileRange ->
                      GcsObjectRange.builder()
                          .setOffset(fileRange.offset())
                          .setLength(fileRange.length())
                          .setByteBufferFuture(fileRange.byteBuffer())
                          .build())
              .collect(Collectors.toList());
      // Count synchronously on the caller (task) thread, before delegating. The range futures
      // complete on analytics-core background threads, so counting in their completion callbacks
      // would attribute the bytes to the wrong thread: under HadoopMetricsContext, READ_BYTES maps
      // to FileSystem.Statistics.incrementBytesRead, which accumulates per-thread, and Spark reads
      // task input bytes from the task thread's statistics. Bytes recorded on a background thread
      // would never reach Spark's task metrics. Correct-thread attribution matters more than
      // precision here, so we count the requested range.length() up front. This over-counts by a
      // bounded amount on a short read near EOF, and counts ranges whose read later fails (the
      // delegate throws below, but the bytes were already recorded); both are acceptable for a
      // metric where the magnitude is right and it lands where Spark can see it.
      for (FileRange range : ranges) {
        if (range.length() > 0) {
          readBytes.increment(range.length());
          readOperations.increment();
        }
      }
      try {
        stream.readVectored(objectRanges, allocate);
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  static class GcsOutputStreamWrapper extends PositionOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(GcsOutputStreamWrapper.class);

    private final StackTraceElement[] createStack;
    private final GoogleCloudStorageOutputStream stream;
    private final BlobId blobId;
    private final Counter writeBytes;
    private final Counter writeOperations;

    private long pos = 0;
    private volatile boolean closed = false;

    GcsOutputStreamWrapper(
        GoogleCloudStorageOutputStream stream, BlobId blobId, MetricsContext metrics) {
      Preconditions.checkArgument(null != stream, "Invalid stream: null");
      Preconditions.checkArgument(null != blobId, "Invalid blobId: null");
      this.stream = stream;
      this.blobId = blobId;
      this.createStack = Thread.currentThread().getStackTrace();
      this.writeBytes =
          metrics.counter(FileIOMetricsContext.WRITE_BYTES, MetricsContext.Unit.BYTES);
      this.writeOperations = metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS);
    }

    @Override
    public synchronized long getPos() {
      return pos;
    }

    @Override
    public synchronized void write(int b) throws IOException {
      stream.write(b);
      pos += 1;
      writeBytes.increment();
      writeOperations.increment();
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
      stream.write(b, off, len);
      pos += len;
      writeBytes.increment(len);
      writeOperations.increment();
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      synchronized (this) {
        if (!closed) {
          closed = true;
          super.close();
          stream.close();
        }
      }
    }

    @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      if (!closed) {
        try {
          close();
        } catch (Throwable t) {
          LOG.warn("Failed to close unclosed stream for {} in finalizer", blobId.toGsUtilUri(), t);
        }
        String trace =
            Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
        LOG.warn("Unclosed output stream for {} created by:\n\t{}", blobId.toGsUtilUri(), trace);
      }
    }
  }
}
