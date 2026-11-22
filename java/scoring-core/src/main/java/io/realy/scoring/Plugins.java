package io.realy.scoring;

import io.realy.common.Env;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Extension jars: every {@code *.jar} in {@code PLUGINS_DIR} (gateway, Flink JobManager and TaskManagers;
 * unset = none) joins the classpath the extension points load from with {@link ServiceLoader}, next to
 * whatever the application's own classpath provides (a jar in Flink's usrlib, a test's META-INF/services).
 * The same directory must hold the same jars everywhere the job's features are computed (gateway and
 * Flink), and every extension needs its Python twin for verify (docs/dev/extending.md).
 */
public final class Plugins {
    private Plugins() {}

    private static volatile ClassLoader loader;

    /** The directory plugins are read from, or null. */
    public static Path dir() {
        String d = Env.str("PLUGINS_DIR", "");
        return d.isEmpty() ? null : Path.of(d);
    }

    /** The jars found in {@link #dir()} (sorted by name), empty when none. */
    public static List<Path> jars() {
        Path d = dir();
        List<Path> out = new ArrayList<>();
        if (d == null || !Files.isDirectory(d)) return out;
        try (Stream<Path> s = Files.list(d)) {
            s.filter(p -> p.getFileName().toString().endsWith(".jar")).sorted().forEach(out::add);
        } catch (IOException e) {
            throw new IllegalStateException("PLUGINS_DIR " + d + " cannot be listed: " + e.getMessage(), e);
        }
        return out;
    }

    /** The class loader extensions are loaded with: the plugin jars over {@code parent}. */
    public static ClassLoader classLoader(ClassLoader parent) {
        ClassLoader l = loader;
        if (l != null) return l;
        synchronized (Plugins.class) {
            if (loader != null) return loader;
            List<Path> jars = jars();
            if (jars.isEmpty()) {
                loader = parent;
            } else {
                URL[] urls = new URL[jars.size()];
                for (int i = 0; i < urls.length; i++) {
                    try {
                        urls[i] = jars.get(i).toUri().toURL();
                    } catch (MalformedURLException e) {
                        throw new IllegalStateException(e);
                    }
                }
                loader = new URLClassLoader("realy-plugins", urls, parent);
            }
            return loader;
        }
    }

    /** Every provider of {@code type}: the application's classpath and the plugin jars. */
    public static <T> ServiceLoader<T> load(Class<T> type) {
        return ServiceLoader.load(type, classLoader(type.getClassLoader()));
    }
}
