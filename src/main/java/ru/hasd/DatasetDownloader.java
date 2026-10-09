package ru.hasd;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.security.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.GZIPInputStream;

public final class DatasetDownloader {
    public record Entry(String file, String url, long bytes, String sha256) {}
    private final Config config;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).followRedirects(HttpClient.Redirect.NORMAL).build();
    public DatasetDownloader(Config config) { this.config = config; }
    public static String filename(Instant time) {
        return DateTimeFormatter.ofPattern("uuuu-MM-dd-H").withZone(ZoneOffset.UTC).format(time) + ".json.gz";
    }
    public static void validateGzip(Path path) throws IOException {
        try (var input = new GZIPInputStream(Files.newInputStream(path))) { input.transferTo(OutputStream.nullOutputStream()); }
    }
    public static String sha256(Path path) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var input = new DigestInputStream(Files.newInputStream(path), digest)) { input.transferTo(OutputStream.nullOutputStream()); }
        return HexFormat.of().formatHex(digest.digest());
    }
    public List<Entry> download() throws Exception {
        Files.createDirectories(config.raw()); Files.createDirectories(config.results());
        var entries = new ArrayList<Entry>(); long total = 0;
        for (int hour = 0; hour < config.maxHours() && total < config.targetBytes(); hour++) {
            String name = filename(config.start().plusSeconds(hour * 3600L));
            String url = "https://data.gharchive.org/" + name;
            Path file = config.raw().resolve(name);
            boolean valid = false;
            if (Files.exists(file)) { try { validateGzip(file); valid = true; } catch (IOException e) { System.out.println("Повреждённый архив будет скачан заново: " + name); } }
            if (!valid) fetch(URI.create(url), file);
            long bytes = Files.size(file); total += bytes;
            entries.add(new Entry(name, url, bytes, sha256(file)));
            writeManifest(entries);
            System.out.printf(Locale.ROOT, "Архивов: %d, размер: %.2f MiB, последний: %s%n", entries.size(), total / 1048576.0, name);
        }
        if (total < config.targetBytes()) throw new IOException("За " + config.maxHours() + " часов получено только " + total + " байт; увеличьте MAX_HOURS");
        return entries;
    }
    private void fetch(URI uri, Path file) throws Exception {
        Path part = file.resolveSibling(file.getFileName() + ".part");
        for (int attempt = 1; attempt <= 4; attempt++) {
            try {
                var request = HttpRequest.newBuilder(uri).timeout(Duration.ofMinutes(15)).GET().build();
                var response = http.send(request, HttpResponse.BodyHandlers.ofFile(part, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
                if (response.statusCode() != 200) throw new IOException("HTTP " + response.statusCode() + ": " + uri);
                validateGzip(part);
                Files.move(part, file, StandardCopyOption.REPLACE_EXISTING);
                return;
            } catch (IOException e) {
                Files.deleteIfExists(part);
                if (attempt == 4) throw e;
                System.out.println("Повтор скачивания " + attempt + ": " + e.getMessage());
                Thread.sleep(1000L << attempt);
            }
        }
    }
    private void writeManifest(List<Entry> entries) throws IOException {
        var lines = new ArrayList<String>(); lines.add("file\turl\tbytes\tsha256");
        for (var e : entries) lines.add(e.file() + "\t" + e.url() + "\t" + e.bytes() + "\t" + e.sha256());
        Path target = config.results().resolve("manifest.tsv"), temp = config.results().resolve("manifest.tsv.part");
        Files.write(temp, lines); Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
    }
    public List<Entry> manifest() throws IOException {
        var rows = Files.readAllLines(config.results().resolve("manifest.tsv"));
        return rows.stream().skip(1).map(line -> { String[] p = line.split("\t"); return new Entry(p[0], p[1], Long.parseLong(p[2]), p[3]); }).toList();
    }
}
