package ru.hasd;

import org.apache.spark.sql.SparkSession;

public final class SparkSessionFactory {
    private SparkSessionFactory() {}
    public static SparkSession create(Config c) {
        var spark = SparkSession.builder().appName("ХАСД: GH Archive ELT")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.hadoop.fs.s3a.endpoint", c.endpoint())
            .config("spark.hadoop.fs.s3a.access.key", c.accessKey())
            .config("spark.hadoop.fs.s3a.secret.key", c.secretKey())
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }
}
