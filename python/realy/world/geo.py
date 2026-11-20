"""Static geography: countries (order = feature_schema.yaml `countries`), cities, currencies, FX.

All tables are small Python literals so the world is fully reproducible without external data.
"""
from __future__ import annotations

import numpy as np

from ..features import load_schema

# country -> (currency, usd_per_unit, minor_exponent, contactless CVM limit in local units,
#             home-customer weight, [(city, lat, lon, weight, tz_offset_min), ...], [asn, ...])
_COUNTRY_DATA: dict[str, tuple] = {
    "US": ("USD", 1.00, 2, 100, 0.18, [("New York", 40.7128, -74.0060, .30, -300), ("Los Angeles", 34.0522, -118.2437, .20, -480),
                                        ("Chicago", 41.8781, -87.6298, .15, -360), ("Houston", 29.7604, -95.3698, .15, -360),
                                        ("Miami", 25.7617, -80.1918, .10, -300), ("Seattle", 47.6062, -122.3321, .10, -480)], [7922, 7018, 701, 20057]),
    "GB": ("GBP", 1.27, 2, 100, 0.30, [("London", 51.5074, -0.1278, .45, 0), ("Manchester", 53.4808, -2.2426, .15, 0),
                                        ("Birmingham", 52.4862, -1.8904, .15, 0), ("Glasgow", 55.8642, -4.2518, .10, 0),
                                        ("Leeds", 53.8008, -1.5491, .08, 0), ("Bristol", 51.4545, -2.5879, .07, 0)], [2856, 5089, 5607, 12576]),
    "DE": ("EUR", 1.08, 2, 50, 0.07, [("Berlin", 52.5200, 13.4050, .30, 60), ("Hamburg", 53.5511, 9.9937, .20, 60),
                                       ("Munich", 48.1351, 11.5820, .20, 60), ("Cologne", 50.9375, 6.9603, .15, 60),
                                       ("Frankfurt", 50.1109, 8.6821, .15, 60)], [3320, 3209, 6805]),
    "FR": ("EUR", 1.08, 2, 50, 0.06, [("Paris", 48.8566, 2.3522, .45, 60), ("Marseille", 43.2965, 5.3698, .15, 60),
                                       ("Lyon", 45.7640, 4.8357, .15, 60), ("Toulouse", 43.6047, 1.4442, .12, 60),
                                       ("Nice", 43.7102, 7.2620, .13, 60)], [3215, 12322, 15557]),
    "ES": ("EUR", 1.08, 2, 50, 0.05, [("Madrid", 40.4168, -3.7038, .40, 60), ("Barcelona", 41.3874, 2.1686, .35, 60),
                                       ("Valencia", 39.4699, -0.3763, .12, 60), ("Seville", 37.3891, -5.9845, .13, 60)], [3352, 12479]),
    "IT": ("EUR", 1.08, 2, 50, 0.04, [("Rome", 41.9028, 12.4964, .35, 60), ("Milan", 45.4642, 9.1900, .35, 60),
                                       ("Naples", 40.8518, 14.2681, .15, 60), ("Turin", 45.0703, 7.6869, .15, 60)], [3269, 30722]),
    "NL": ("EUR", 1.08, 2, 50, 0.03, [("Amsterdam", 52.3676, 4.9041, .45, 60), ("Rotterdam", 51.9244, 4.4777, .25, 60),
                                       ("The Hague", 52.0705, 4.3007, .15, 60), ("Utrecht", 52.0907, 5.1214, .15, 60)], [1136, 33915]),
    "PL": ("PLN", 0.25, 2, 100, 0.03, [("Warsaw", 52.2297, 21.0122, .40, 60), ("Krakow", 50.0647, 19.9450, .25, 60),
                                        ("Wroclaw", 51.1079, 17.0385, .20, 60), ("Gdansk", 54.3520, 18.6466, .15, 60)], [5617, 12741]),
    "SE": ("SEK", 0.095, 2, 400, 0.02, [("Stockholm", 59.3293, 18.0686, .50, 60), ("Gothenburg", 57.7089, 11.9746, .30, 60),
                                         ("Malmo", 55.6050, 13.0038, .20, 60)], [3301, 1257]),
    "IE": ("EUR", 1.08, 2, 50, 0.03, [("Dublin", 53.3498, -6.2603, .60, 0), ("Cork", 51.8985, -8.4756, .25, 0),
                                       ("Galway", 53.2707, -9.0568, .15, 0)], [5466, 6830]),
    "CA": ("CAD", 0.73, 2, 250, 0.03, [("Toronto", 43.6532, -79.3832, .40, -300), ("Montreal", 45.5017, -73.5673, .25, -300),
                                        ("Vancouver", 49.2827, -123.1207, .20, -480), ("Calgary", 51.0447, -114.0719, .15, -420)], [577, 812]),
    "MX": ("MXN", 0.058, 2, 400, 0.015, [("Mexico City", 19.4326, -99.1332, .50, -360), ("Guadalajara", 20.6597, -103.3496, .25, -360),
                                          ("Monterrey", 25.6866, -100.3161, .25, -360)], [8151, 28403]),
    "BR": ("BRL", 0.20, 2, 200, 0.015, [("Sao Paulo", -23.5505, -46.6333, .50, -180), ("Rio de Janeiro", -22.9068, -43.1729, .35, -180),
                                         ("Brasilia", -15.7975, -47.8919, .15, -180)], [28573, 18881]),
    "IN": ("INR", 0.012, 2, 5000, 0.02, [("Mumbai", 19.0760, 72.8777, .35, 330), ("Delhi", 28.7041, 77.1025, .35, 330),
                                          ("Bangalore", 12.9716, 77.5946, .30, 330)], [9829, 55836, 45609]),
    "JP": ("JPY", 0.0067, 0, 15000, 0.01, [("Tokyo", 35.6762, 139.6503, .60, 540), ("Osaka", 34.6937, 135.5023, .40, 540)], [2516, 4713]),
    "AU": ("AUD", 0.66, 2, 200, 0.02, [("Sydney", -33.8688, 151.2093, .50, 660), ("Melbourne", -37.8136, 144.9631, .50, 660)], [1221, 4804]),
    "SG": ("SGD", 0.74, 2, 200, 0.01, [("Singapore", 1.3521, 103.8198, 1.0, 480)], [9506, 4773]),
    "AE": ("AED", 0.272, 2, 500, 0.01, [("Dubai", 25.2048, 55.2708, .70, 240), ("Abu Dhabi", 24.4539, 54.3773, .30, 240)], [5384, 15802]),
    "NG": ("NGN", 0.00065, 2, 50000, 0.01, [("Lagos", 6.5244, 3.3792, .70, 60), ("Abuja", 9.0765, 7.3986, .30, 60)], [29465, 37148]),
    "ZA": ("ZAR", 0.054, 2, 500, 0.01, [("Johannesburg", -26.2041, 28.0473, .60, 120), ("Cape Town", -33.9249, 18.4241, .40, 120)], [3741, 37457]),
    "TR": ("TRY", 0.031, 2, 1500, 0.01, [("Istanbul", 41.0082, 28.9784, .70, 180), ("Ankara", 39.9334, 32.8597, .30, 180)], [9121, 16135]),
    "RO": ("RON", 0.22, 2, 200, 0.01, [("Bucharest", 44.4268, 26.1025, .70, 120), ("Cluj-Napoca", 46.7712, 23.6236, .30, 120)], [8708, 6830]),
    "UA": ("UAH", 0.024, 2, 1500, 0.005, [("Kyiv", 50.4501, 30.5234, .70, 120), ("Lviv", 49.8397, 24.0297, .30, 120)], [15895, 13188]),
    "CN": ("CNY", 0.14, 2, 1000, 0.005, [("Shanghai", 31.2304, 121.4737, .50, 480), ("Beijing", 39.9042, 116.4074, .50, 480)], [4134, 4837]),
}

# Hosting / VPN ASNs used by fraudsters (never assigned to legitimate home IPs).
HOSTING_ASNS = np.array([14061, 16509, 24940, 9009, 60068, 212238], dtype=np.uint32)
# Countries fraud rings typically operate from in this simulation (indices resolved lazily).
FRAUD_SOURCE_COUNTRIES = ["NG", "RO", "UA", "BR", "IN", "CN", "TR", "MX"]


class Geo:
    """Vectorised lookup tables derived from the static data, indexed by country / city index."""

    def __init__(self) -> None:
        countries = load_schema().countries
        missing = [c for c in countries if c not in _COUNTRY_DATA]
        if missing:
            raise ValueError(f"no geo data for countries {missing}")
        self.countries: list[str] = list(countries)
        n = len(countries)
        self.currency = [_COUNTRY_DATA[c][0] for c in countries]
        self.fx_usd = np.array([_COUNTRY_DATA[c][1] for c in countries], dtype=np.float64)
        self.minor_exp = np.array([_COUNTRY_DATA[c][2] for c in countries], dtype=np.int64)
        self.minor_mult = (10 ** self.minor_exp).astype(np.float64)
        # CVM limit in USD cents (from local units)
        self.cvm_usd_minor = np.array([round(_COUNTRY_DATA[c][3] * _COUNTRY_DATA[c][1] * 100) for c in countries], dtype=np.int64)
        w = np.array([_COUNTRY_DATA[c][4] for c in countries], dtype=np.float64)
        self.home_weight = w / w.sum()
        cities: list[tuple[int, str, float, float, float, int]] = []
        for ci, c in enumerate(countries):
            for (name, lat, lon, cw, tz) in _COUNTRY_DATA[c][5]:
                cities.append((ci, name, lat, lon, cw, tz))
        self.city_names = [x[1] for x in cities]
        self.city_country = np.array([x[0] for x in cities], dtype=np.int16)
        self.city_lat = np.array([x[2] for x in cities], dtype=np.float64)
        self.city_lon = np.array([x[3] for x in cities], dtype=np.float64)
        self.city_weight = np.array([x[4] for x in cities], dtype=np.float64)  # within country
        self.city_tz = np.array([x[5] for x in cities], dtype=np.int16)
        self.n_countries = n
        self.n_cities = len(cities)
        self.country_cities = [np.flatnonzero(self.city_country == ci) for ci in range(n)]
        self.country_asns = [np.array(_COUNTRY_DATA[c][6], dtype=np.uint32) for c in countries]
        # IPv4 block per country: first octet = 11 + 9*idx  (stays inside 0..255 for 24 countries)
        self.ip_first_octet = np.array([11 + 9 * i for i in range(n)], dtype=np.uint32)
        self.fraud_source = np.array([countries.index(c) for c in FRAUD_SOURCE_COUNTRIES if c in countries], dtype=np.int16)
        # capital-ish city per country = highest weight city
        self.main_city = np.array([cc[np.argmax(self.city_weight[cc])] for cc in self.country_cities], dtype=np.int32)
        # global city weight (country home weight x city weight), used for merchant placement
        cw = self.city_weight * self.home_weight[self.city_country]
        self.city_global_weight = cw / cw.sum()

    def country_code(self, idx: np.ndarray) -> np.ndarray:
        return np.asarray(self.countries, dtype="U2")[idx]

    def currency_of(self, idx: np.ndarray) -> np.ndarray:
        return np.asarray(self.currency, dtype="U3")[idx]


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance (km). Same formula as the Java/Python feature engines."""
    r = 6371.0088
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi = p2 - p1
    dl = np.radians(np.asarray(lon2) - np.asarray(lon1))
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(np.minimum(1.0, a)))


_GEO: Geo | None = None


def geo() -> Geo:
    global _GEO
    if _GEO is None:
        _GEO = Geo()
    return _GEO
