package lab.m1;

import java.nio.file.*;
import java.io.*;
import java.util.Properties;

final class Shared {
    static final Path ROOT = Path.of("/var/run/kafka-health");
    static final String DEFAULT_TOPIC = "health-validation-m1-b2";
    static String target() throws IOException {
        Path file = ROOT.resolve("target");
        return Files.exists(file) ? Files.readString(file).trim() : DEFAULT_TOPIC;
    }
    static Properties read(String name) throws IOException {
        Properties p = new Properties();
        try (var in = Files.newInputStream(ROOT.resolve(name))) { p.load(in); }
        return p;
    }
    static void write(String name, Properties p) throws IOException {
        Path temp = ROOT.resolve(name + ".new");
        try (var out = Files.newOutputStream(temp)) { p.store(out, null); }
        Files.move(temp, ROOT.resolve(name), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
    static long number(Properties p, String key) {
        return Long.parseLong(p.getProperty(key, "-1"));
    }
}
