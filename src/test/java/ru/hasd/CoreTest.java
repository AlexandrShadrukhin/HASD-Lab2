package ru.hasd;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Instant;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import static org.junit.jupiter.api.Assertions.*;

class CoreTest {
    @TempDir Path temp;
    @Test void utcNamesCrossDateBoundary() {
        assertEquals("2024-10-01-0.json.gz", DatasetDownloader.filename(Instant.parse("2024-10-01T00:00:00Z")));
        assertEquals("2024-10-02-0.json.gz", DatasetDownloader.filename(Instant.parse("2024-10-01T23:00:00Z").plusSeconds(3600)));
    }
    @Test void gzipDetectsCorruptedTrailer() throws Exception {
        Path path = temp.resolve("data.gz");
        try (var out = new GZIPOutputStream(Files.newOutputStream(path))) { out.write("данные".getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
        assertDoesNotThrow(() -> DatasetDownloader.validateGzip(path));
        byte[] bytes = Files.readAllBytes(path); bytes[bytes.length - 8] ^= 1;
        Files.write(path, bytes);
        assertThrows(java.io.IOException.class, () -> DatasetDownloader.validateGzip(path));
    }
    @Test void hashMatchesKnownVector() throws Exception {
        Path path = temp.resolve("abc"); Files.writeString(path, "abc");
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", DatasetDownloader.sha256(path));
    }
    @Test void medianHandlesOddEvenAndEmpty() {
        assertEquals(3, MetricsCollector.median(List.of(5.0, 1.0, 3.0)));
        assertEquals(2, MetricsCollector.median(List.of(3.0, 1.0)));
        assertThrows(IllegalArgumentException.class, () -> MetricsCollector.median(List.of()));
    }
    private Config downloadConfig(long target, int maxHours) {
        return new Config(temp.resolve("raw"), temp.resolve("results"), target, maxHours,
            Instant.parse("2024-10-01T00:00:00Z"), "", "", "", "smoke");
    }
    private Path archive(String name) throws Exception {
        Files.createDirectories(temp.resolve("raw"));
        Path path = temp.resolve("raw").resolve(name);
        try (var out = new GZIPOutputStream(Files.newOutputStream(path))) { out.write("{\"id\":\"test\"}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
        return path;
    }
    @Test void resumeUsesValidArchiveAndStopsAtTarget() throws Exception {
        Path first = archive("2024-10-01-0.json.gz");
        archive("2024-10-01-1.json.gz");
        var before = Files.getLastModifiedTime(first);
        var downloader = new DatasetDownloader(downloadConfig(Files.size(first), 2));
        var entries = downloader.download();
        assertEquals(1, entries.size());
        assertEquals(Files.size(first), entries.get(0).bytes());
        assertEquals(before, Files.getLastModifiedTime(first));
        assertEquals(entries, downloader.manifest());
        assertEquals(entries, downloader.download());
    }
    @Test void insufficientRangeProducesErrorAndKeepsManifest() throws Exception {
        Path first = archive("2024-10-01-0.json.gz");
        var downloader = new DatasetDownloader(downloadConfig(Files.size(first) + 1, 1));
        var error = assertThrows(java.io.IOException.class, downloader::download);
        assertTrue(error.getMessage().contains("MAX_HOURS"));
        assertEquals(1, downloader.manifest().size());
        assertTrue(Files.exists(first));
    }
}
