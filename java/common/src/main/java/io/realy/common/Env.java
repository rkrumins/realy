package io.realy.common;

/** Environment-variable configuration with defaults (12-factor; same names in compose and K8s). */
public final class Env {
    private Env() {}

    public static String str(String name, String def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) v = System.getProperty(name);
        return v == null || v.isBlank() ? def : v.trim();
    }

    public static int integer(String name, int def) {
        return Integer.parseInt(str(name, Integer.toString(def)));
    }

    public static long lng(String name, long def) {
        return Long.parseLong(str(name, Long.toString(def)));
    }

    public static boolean bool(String name, boolean def) {
        return Boolean.parseBoolean(str(name, Boolean.toString(def)));
    }

    public static double dbl(String name, double def) {
        return Double.parseDouble(str(name, Double.toString(def)));
    }
}
