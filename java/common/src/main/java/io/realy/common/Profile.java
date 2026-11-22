package io.realy.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * REALY_PROFILE: `bench` (default: the benchmark drives the services) or `production` (the same services
 * processing real payments, with nothing of the benchmark reachable):
 *
 * <ul>
 *   <li>the control topic (bench.control) is not subscribed unless CONTROL_ENABLED=true;
 *   <li>admin writes (PUT /admin/config) need ADMIN_TOKEN(_FILE), and are refused without one;
 *   <li>benchmark-only scoring settings are refused: echo, injected latency or errors, the mock store;
 *   <li>the model's input width must come from its metadata or MODEL_INPUT_DIM (no guessing).
 * </ul>
 *
 * In the bench profile admin writes also need the token when ADMIN_TOKEN is set, and ADMIN_WRITE=false
 * refuses them.
 */
public final class Profile {
    private Profile() {}

    public static boolean production() {
        return "production".equalsIgnoreCase(Env.str("REALY_PROFILE", "bench"));
    }

    public static boolean controlEnabled() {
        return Env.bool("CONTROL_ENABLED", !production());
    }

    /** The admin token (ADMIN_TOKEN_FILE, else ADMIN_TOKEN); "" when none. */
    static String adminToken() {
        String f = Env.str("ADMIN_TOKEN_FILE", "");
        if (!f.isBlank()) {
            try {
                return Files.readString(Path.of(f)).trim();
            } catch (IOException e) {
                throw new java.io.UncheckedIOException("ADMIN_TOKEN_FILE", e);
            }
        }
        return Env.str("ADMIN_TOKEN", "");
    }

    /**
     * Null when an admin write with this Authorization header is allowed, else why not (for a 401 / 403).
     */
    public static String adminWriteRefusal(String authorization) {
        if (!Env.bool("ADMIN_WRITE", true)) return "admin writes are disabled (ADMIN_WRITE=false)";
        String token = adminToken();
        if (token.isBlank()) return production() ? "admin writes need ADMIN_TOKEN in the production profile" : null;
        String want = "Bearer " + token;
        if (authorization == null || !java.security.MessageDigest.isEqual(
                want.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                authorization.trim().getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            return "a valid Authorization: Bearer <ADMIN_TOKEN> header is required";
        }
        return null;
    }
}
