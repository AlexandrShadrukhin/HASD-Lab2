package ru.hasd;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import java.util.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public final class TransformJob {
    private TransformJob() {}
    public static StructType schema() {
        return new StructType().add("id", StringType).add("type", StringType)
            .add("actor", new StructType().add("id", LongType).add("login", StringType))
            .add("repo", new StructType().add("id", LongType).add("name", StringType))
            .add("created_at", StringType).add("_corrupt_record", StringType);
    }
    public static Dataset<Row> raw(SparkSession spark, Config c, List<DatasetDownloader.Entry> entries) {
        String[] paths = entries.stream().map(e -> "s3a://raw/" + c.prefix() + e.file()).toArray(String[]::new);
        return spark.read().schema(schema()).option("mode", "PERMISSIVE").json(paths);
    }
    public static Dataset<Row> flatten(Dataset<Row> raw) {
        return raw.select(col("id").alias("event_id"), col("type").alias("event_type"),
            col("actor.id").alias("actor_id"), col("actor.login").alias("actor_login"),
            col("repo.id").alias("repo_id"), col("repo.name").alias("repo_name"),
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ssX").alias("created_at"), col("_corrupt_record"))
            .withColumn("event_date", to_date(col("created_at")))
            .withColumn("event_hour", hour(col("created_at")))
            .withColumn("event_day_of_week", expr("pmod(dayofweek(created_at) + 5, 7) + 1"));
    }
    public static Dataset<Row> valid(Dataset<Row> flat) {
        return flat.filter("_corrupt_record IS NULL AND event_id IS NOT NULL AND trim(event_id) <> '' AND event_type IS NOT NULL AND trim(event_type) <> '' AND created_at IS NOT NULL").drop("_corrupt_record");
    }
    public static Dataset<Row> clean(Dataset<Row> raw) { return valid(flatten(raw)).dropDuplicates("event_id"); }
    public static Map<String, Object> run(SparkSession spark, Config c, List<DatasetDownloader.Entry> entries) {
        Dataset<Row> flat = flatten(raw(spark, c, entries)).persist(StorageLevel.DISK_ONLY());
        long input = flat.count();
        long corrupt = flat.filter("_corrupt_record IS NOT NULL").count();
        Dataset<Row> valid = valid(flat); long validCount = valid.count();
        Dataset<Row> unique = valid.dropDuplicates("event_id").persist(StorageLevel.DISK_ONLY());
        long output = unique.count();
        if (output == 0) throw new IllegalStateException("После очистки не осталось событий");
        unique.repartition(4).write().mode(SaveMode.Overwrite).partitionBy("event_date").parquet(c.curated());
        unique.unpersist(); flat.unpersist();
        var metrics = new LinkedHashMap<String, Object>();
        metrics.put("input_rows", input); metrics.put("corrupt_rows", corrupt);
        metrics.put("invalid_rows", input - validCount); metrics.put("duplicate_rows", validCount - output);
        metrics.put("curated_rows", output);
        ValidationService.validate(spark.read().parquet(c.curated()), output);
        return metrics;
    }
}
