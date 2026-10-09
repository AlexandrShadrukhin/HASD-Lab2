package ru.hasd;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import java.nio.file.*;
import java.util.*;

public final class RawDataLoader implements AutoCloseable {
    private final Config config;
    private final AmazonS3 s3;
    public RawDataLoader(Config config) {
        this.config = config;
        if (config.accessKey().isBlank() || config.secretKey().isBlank()) throw new IllegalArgumentException("Не заданы ключи MinIO");
        s3 = AmazonS3ClientBuilder.standard().withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.endpoint(), "us-east-1"))
            .withPathStyleAccessEnabled(true).withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.accessKey(), config.secretKey())))
            .withClientConfiguration(new ClientConfiguration().withMaxErrorRetry(4).withConnectionTimeout(15000).withSocketTimeout(120000)).build();
    }
    public void load(List<DatasetDownloader.Entry> entries) throws Exception {
        int uploaded = 0, skipped = 0;
        for (var e : entries) {
            Path file = config.raw().resolve(e.file());
            if (Files.size(file) != e.bytes() || !DatasetDownloader.sha256(file).equals(e.sha256())) throw new IllegalStateException("Файл изменился: " + file);
            String key = config.prefix() + e.file(); ObjectMetadata existing = null;
            try { existing = s3.getObjectMetadata("raw", key); } catch (AmazonS3Exception ex) { if (ex.getStatusCode() != 404) throw ex; }
            if (existing != null && existing.getContentLength() == e.bytes() && e.sha256().equals(existing.getUserMetaDataOf("sha256"))) { skipped++; continue; }
            var metadata = new ObjectMetadata(); metadata.setContentType("application/gzip");
            metadata.addUserMetadata("sha256", e.sha256()); metadata.addUserMetadata("source", e.url());
            s3.putObject(new PutObjectRequest("raw", key, file.toFile()).withMetadata(metadata)); uploaded++;
        }
        verify(entries);
        s3.putObject("raw", config.prefix() + "manifest.tsv", config.results().resolve("manifest.tsv").toFile());
        System.out.printf("RAW: загружено %d, идентичных пропущено %d%n", uploaded, skipped);
    }
    public void initializeBuckets() {
        for (String bucket : List.of("raw", "curated", "analytics")) {
            if (!s3.doesBucketExistV2(bucket)) s3.createBucket(bucket);
            if (!s3.doesBucketExistV2(bucket)) throw new IllegalStateException("Bucket не создан: " + bucket);
            System.out.println("Bucket готов: " + bucket);
        }
    }
    public void verify(List<DatasetDownloader.Entry> entries) {
        long total = 0;
        for (var e : entries) {
            var m = s3.getObjectMetadata("raw", config.prefix() + e.file());
            if (m.getContentLength() != e.bytes() || !e.sha256().equals(m.getUserMetaDataOf("sha256"))) throw new IllegalStateException("Не совпадает RAW: " + e.file());
            total += m.getContentLength();
        }
        if (total < config.targetBytes()) throw new IllegalStateException("Недостаточный размер RAW: " + total);
    }
    public long parquetBytes() {
        long total = 0; String token = null;
        do {
            var page = s3.listObjectsV2(new ListObjectsV2Request().withBucketName("curated").withPrefix(config.prefix() + "events/").withContinuationToken(token));
            for (var object : page.getObjectSummaries()) if (object.getKey().endsWith(".parquet")) total += object.getSize();
            token = page.getNextContinuationToken();
        } while (token != null);
        return total;
    }
    @Override public void close() { s3.shutdown(); }
}
