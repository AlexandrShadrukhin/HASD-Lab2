package ru.hasd;

import org.apache.spark.sql.*;

/** Небольшая фикстура проверяет правила очистки, но не используется как датасет лабораторной. */
public final class TransformIntegrationTest {
    private TransformIntegrationTest() {}
    public static void run(SparkSession spark) {
        var input = spark.createDataset(java.util.List.of(
            "{\"id\":\"1\",\"type\":\"PushEvent\",\"created_at\":\"2024-10-01T23:01:02Z\"}",
            "{\"id\":\"1\",\"type\":\"PushEvent\",\"created_at\":\"2024-10-01T23:01:02Z\"}",
            "{\"id\":\"2\",\"type\":\"WatchEvent\",\"created_at\":\"2024-10-02T00:00:00Z\"}",
            "{\"id\":\"3\",\"type\":\"PushEvent\",\"created_at\":\"bad\"}",
            "{\"id\":\"4\",\"created_at\":\"2024-10-01T00:00:00Z\"}",
            "broken"), Encoders.STRING());
        var clean = TransformJob.clean(spark.read().schema(TransformJob.schema()).json(input));
        ValidationService.validate(clean, 2);
        var row = clean.filter("event_id = '1'").first();
        if (row.<Integer>getAs("event_hour") != 23 || row.<Integer>getAs("event_day_of_week") != 2 || row.getAs("actor_id") != null) throw new AssertionError("Неверные вычисляемые или необязательные поля");
        System.out.println("Интеграционный тест очистки пройден");
    }
}
