package io.realy.scoring.features;

/** Haversine distance; identical formula/operation order in python/realy/features/geo.py. */
public final class Geo {
    public static final double EARTH_RADIUS_KM = 6371.0088;

    private Geo() {}

    public static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double p1 = Math.toRadians(lat1);
        double p2 = Math.toRadians(lat2);
        double dp = p2 - p1;
        double dl = Math.toRadians(lon2) - Math.toRadians(lon1);
        double s1 = Math.sin(dp / 2.0);
        double s2 = Math.sin(dl / 2.0);
        double a = s1 * s1 + Math.cos(p1) * Math.cos(p2) * s2 * s2;
        return 2.0 * EARTH_RADIUS_KM * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }
}
