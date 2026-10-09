package ru.hasd;

import java.nio.file.*;
import java.util.*;
import java.io.IOException;

public final class MetricsCollector {
    private MetricsCollector() {}
    public static double median(List<Double> values) {
        if (values.isEmpty()) throw new IllegalArgumentException("Нет измерений");
        var sorted = values.stream().sorted().toList(); int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
    }
    public static void save(Config c, Map<String, Object> metrics) throws IOException {
        Files.createDirectories(c.results());
        var lines = new ArrayList<String>(); lines.add("metric\tvalue");
        metrics.forEach((key, value) -> lines.add(key + "\t" + value));
        Files.write(c.results().resolve("metrics.tsv"), lines);
        metrics.forEach((key, value) -> System.out.println(key + ": " + value));
    }
}
