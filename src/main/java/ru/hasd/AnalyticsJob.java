package ru.hasd;

import org.apache.spark.sql.*;
import java.nio.file.*;
import java.util.*;

public final class AnalyticsJob {
    private AnalyticsJob() {}
    public static Map<String, String> queries() {
        var q = new LinkedHashMap<String, String>();
        q.put("event_types", "SELECT event_type, count(*) AS events FROM events GROUP BY event_type ORDER BY events DESC, event_type");
        q.put("hourly", "SELECT event_date, event_hour, count(*) AS events FROM events GROUP BY event_date, event_hour ORDER BY event_date, event_hour");
        q.put("daily", "SELECT event_date, count(*) AS events FROM events GROUP BY event_date ORDER BY event_date");
        q.put("top_repositories", "SELECT repo_name, count(*) AS events FROM events WHERE repo_name IS NOT NULL GROUP BY repo_name ORDER BY events DESC, repo_name LIMIT 20");
        q.put("unique_entities", "SELECT count(DISTINCT actor_id) AS users, count(DISTINCT repo_id) AS repositories FROM events");
        q.put("types_by_day", "SELECT event_date, event_type, count(*) AS events FROM events GROUP BY event_date, event_type ORDER BY event_date, events DESC, event_type");
        return q;
    }
    public static void run(SparkSession spark, Config c) throws Exception {
        var events = spark.read().parquet(c.curated());
        events.createOrReplaceTempView("events");
        long total = events.count();
        for (var query : queries().entrySet()) {
            var result = spark.sql(query.getValue());
            System.out.println("Аналитика: " + query.getKey()); result.show(30, false);
            var rows = result.toJSON().collectAsList();
            if (rows.isEmpty()) throw new IllegalStateException("Пустая аналитика: " + query.getKey());
            validateAggregate(query.getKey(), result, total);
            Files.write(c.results().resolve(query.getKey() + ".jsonl"), rows);
            String path = "s3a://analytics/" + c.prefix() + query.getKey();
            result.coalesce(1).write().mode("overwrite").option("header", true).csv(path);
            var saved = spark.read().option("header", true).csv(path);
            if (saved.count() != rows.size() || !Arrays.equals(saved.columns(), result.columns())) throw new IllegalStateException("Не совпадает сохранённая аналитика: " + query.getKey());
        }
        System.out.println("Проверены агрегаты и повторное чтение шести CSV из MinIO");
    }
    private static void validateAggregate(String name, Dataset<Row> result, long total) {
        var rows = result.collectAsList();
        if (name.equals("unique_entities")) {
            Row row = rows.get(0);
            long users = row.getLong(0), repositories = row.getLong(1);
            if (rows.size() != 1 || users < 0 || users > total || repositories < 0 || repositories > total) throw new IllegalStateException("Некорректные счётчики уникальных объектов");
            return;
        }
        long sum = 0;
        for (Row row : rows) {
            long count = row.getLong(row.fieldIndex("events"));
            if (count <= 0) throw new IllegalStateException("Некорректный счётчик: " + name);
            sum += count;
        }
        if (name.equals("top_repositories")) {
            if (rows.size() > 20 || sum > total) throw new IllegalStateException("Некорректный Top-20");
        } else if (sum != total) throw new IllegalStateException("Сумма агрегата не совпадает с curated: " + name);
    }
}
