package ru.hasd;

import java.nio.file.Path;
import java.time.Instant;

public record Config(Path raw, Path results, long targetBytes, int maxHours, Instant start,
                     String endpoint, String accessKey, String secretKey, String mode) {
    public static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    public static Config load() {
        String mode = env("RUN_MODE", "full");
        long mib = Long.parseLong(env("TARGET_MIB", mode.equals("smoke") ? "1" : "550"));
        int hours = Integer.parseInt(env("MAX_HOURS", "168"));
        if (mib <= 0 || hours <= 0 || hours > 8760) throw new IllegalArgumentException("Размер и диапазон должны быть положительными; максимум 8760 часов");
        if (!mode.equals("full") && !mode.equals("smoke")) throw new IllegalArgumentException("Режим: full или smoke");
        if (mode.equals("full") && mib < 500) throw new IllegalArgumentException("Полная проверка требует минимум 500 MiB");
        Instant start = Instant.parse(env("START_UTC", "2024-10-01T00:00:00Z"));
        if (start.getEpochSecond() % 3600 != 0 || start.getNano() != 0) throw new IllegalArgumentException("Начало должно совпадать с началом часа UTC");
        return new Config(Path.of(env("RAW_DIR", "data/raw")), Path.of(env("RESULTS_DIR", "results/" + mode)),
            Math.multiplyExact(mib, 1024L * 1024), hours, start, env("S3_ENDPOINT", "http://minio:9000"),
            env("MINIO_ROOT_USER", ""), env("MINIO_ROOT_PASSWORD", ""), mode);
    }
    public String prefix() { return "gharchive/" + mode + "/"; }
    public String curated() { return "s3a://curated/" + prefix() + "events"; }
}
