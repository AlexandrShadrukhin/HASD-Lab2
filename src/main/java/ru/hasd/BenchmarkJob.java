package ru.hasd;

import org.apache.spark.sql.*;
import java.nio.file.*;
import java.util.*;

public final class BenchmarkJob {
    private BenchmarkJob() {}
    public static void run(SparkSession spark, Config c, List<DatasetDownloader.Entry> entries, Map<String, Object> metrics) throws Exception {
        String date = c.start().toString().substring(0, 10);
        var queries = List.of(
            "SELECT event_type, count(*) AS n FROM bench WHERE event_date = DATE '" + date + "' GROUP BY event_type ORDER BY event_type",
            "SELECT repo_name, count(*) AS n FROM bench WHERE event_date = DATE '" + date + "' AND repo_name IS NOT NULL GROUP BY repo_name ORDER BY n DESC, repo_name LIMIT 20");
        var lines = new ArrayList<String>(); lines.add("query,round,format,seconds");
        for (int q = 0; q < queries.size(); q++) {
            var rawTimes = new ArrayList<Double>(); var parquetTimes = new ArrayList<Double>();
            List<Row> reference = null;
            for (int round = 0; round < 4; round++) {
                for (int position = 0; position < 2; position++) {
                    boolean raw = (round + position) % 2 == 0;
                    spark.catalog().clearCache();
                    long start = System.nanoTime();
                    Dataset<Row> data = raw ? TransformJob.clean(TransformJob.raw(spark, c, entries)) : spark.read().parquet(c.curated());
                    data.createOrReplaceTempView("bench");
                    var result = spark.sql(queries.get(q));
                    var rows = result.collectAsList();
                    double seconds = (System.nanoTime() - start) / 1e9;
                    System.out.printf(Locale.ROOT, "Benchmark: запрос %d, %s, %s, %.3f с%n", q,
                        round == 0 ? "прогрев" : "замер " + round, raw ? "RAW JSON.GZ" : "Parquet", seconds);
                    if (reference == null) reference = rows;
                    else if (!reference.equals(rows)) throw new IllegalStateException("RAW и Parquet дали разные результаты benchmark");
                    if (round > 0) (raw ? rawTimes : parquetTimes).add(seconds);
                    lines.add(q + "," + round + "," + (raw ? "raw" : "parquet") + "," + seconds);
                    if (round == 0) Files.writeString(c.results().resolve("plan-" + q + "-" + (raw ? "raw" : "parquet") + ".txt"), result.queryExecution().toString());
                }
            }
            metrics.put("benchmark_" + q + "_raw_median_s", MetricsCollector.median(rawTimes));
            metrics.put("benchmark_" + q + "_parquet_median_s", MetricsCollector.median(parquetTimes));
        }
        Files.write(c.results().resolve("benchmark.csv"), lines);
    }
}
