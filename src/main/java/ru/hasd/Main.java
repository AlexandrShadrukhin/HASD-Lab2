package ru.hasd;

import java.nio.file.*;
import java.util.*;

public final class Main {
    public static void main(String[] args) throws Exception {
        if (Runtime.version().feature() != 17) throw new IllegalStateException("Требуется Java 17");
        Config c = Config.load(); Files.createDirectories(c.results());
        String command = args.length == 0 ? "pipeline" : args[0];
        var downloader = new DatasetDownloader(c);
        if (command.equals("download")) { downloader.download(); return; }
        if (command.equals("init")) { try (var loader = new RawDataLoader(c)) { loader.initializeBuckets(); } return; }
        if (command.equals("transform-test")) { try (var spark = SparkSessionFactory.create(c)) { TransformIntegrationTest.run(spark); } return; }
        var entries = downloader.manifest();
        try (var loader = new RawDataLoader(c)) {
            if (command.equals("load")) { loader.load(entries); return; }
            if (!Set.of("pipeline", "validate", "analytics").contains(command)) throw new IllegalArgumentException("Неизвестная команда: " + command);
            loader.verify(entries);
            try (var spark = SparkSessionFactory.create(c)) {
                if (command.equals("validate")) {
                    long expected = -1;
                    Path metricsFile = c.results().resolve("metrics.tsv");
                    if (Files.exists(metricsFile)) {
                        expected = Files.readAllLines(metricsFile).stream().filter(line -> line.startsWith("curated_rows\t"))
                            .mapToLong(line -> Long.parseLong(line.split("\t")[1])).findFirst().orElse(-1);
                    }
                    ValidationService.validate(spark.read().parquet(c.curated()), expected);
                    return;
                }
                if (command.equals("analytics")) { AnalyticsJob.run(spark, c); return; }
                var metrics = new LinkedHashMap<String, Object>();
                metrics.put("mode", c.mode()); metrics.put("source", "https://www.gharchive.org/");
                metrics.put("started_utc", java.time.Instant.now());
                metrics.put("java_version", System.getProperty("java.runtime.version"));
                metrics.put("spark_version", spark.version());
                metrics.put("hadoop_version", org.apache.hadoop.util.VersionInfo.getVersion());
                metrics.put("target_bytes", c.targetBytes());
                metrics.put("benchmark_warmup_runs", 1); metrics.put("benchmark_measured_runs", 3);
                metrics.put("start_utc", c.start()); metrics.put("end_utc_exclusive", c.start().plusSeconds(entries.size() * 3600L));
                metrics.put("raw_files", entries.size()); metrics.put("raw_bytes", entries.stream().mapToLong(DatasetDownloader.Entry::bytes).sum());
                metrics.putAll(TransformJob.run(spark, c, entries));
                metrics.put("parquet_bytes", loader.parquetBytes());
                MetricsCollector.save(c, metrics);
                AnalyticsJob.run(spark, c);
                BenchmarkJob.run(spark, c, entries, metrics);
                metrics.put("completed_utc", java.time.Instant.now());
                MetricsCollector.save(c, metrics);
            }
        }
    }
}
