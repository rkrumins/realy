package io.realy.scoring.model;

import com.fasterxml.jackson.databind.JsonNode;
import io.realy.common.ProtoJson;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Structural proxy model: standardisation + dense MLP layers in float32, loaded from the weights JSON
 * that `realy train` exports alongside the ONNX model for Triton (models/proxy/*.weights.json). The
 * report recomputes scores from the logged features with the same weights (numpy float32).
 */
public final class ProxyModel {
    public record Layer(float[][] w, float[] b, String activation) {}

    public final String name;
    public final String version;
    public final int nFeatures;
    private final float[] mean;
    private final float[] std;
    private final List<Layer> layers;

    private ProxyModel(String name, String version, int n, float[] mean, float[] std, List<Layer> layers) {
        this.name = name;
        this.version = version;
        this.nFeatures = n;
        this.mean = mean;
        this.std = std;
        this.layers = layers;
    }

    public static ProxyModel parse(String json) {
        try {
            JsonNode r = ProtoJson.jsonMapper().readTree(json);
            int n = r.get("n_features").asInt();
            List<Layer> layers = new ArrayList<>();
            for (JsonNode l : r.get("layers")) {
                JsonNode wn = l.get("W");
                float[][] w = new float[wn.size()][];
                for (int i = 0; i < wn.size(); i++) w[i] = floats(wn.get(i));
                layers.add(new Layer(w, floats(l.get("b")), l.path("activation").asText("relu")));
            }
            return new ProxyModel(r.path("name").asText("fraud_mlp"), r.path("version").asText("0"), n,
                    floats(r.get("mean")), floats(r.get("std")), layers);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static float[] floats(JsonNode a) {
        float[] f = new float[a.size()];
        for (int i = 0; i < f.length; i++) f[i] = (float) a.get(i).asDouble();
        return f;
    }

    /** MODEL_WEIGHTS path, else the bundled default (models/proxy/default.weights.json). */
    public static ProxyModel load(String path) throws IOException {
        if (path != null && !path.isBlank()) return parse(Files.readString(Path.of(path)));
        try (InputStream in = ProxyModel.class.getClassLoader().getResourceAsStream("realy/models/default.weights.json")) {
            if (in == null) throw new IllegalStateException("default.weights.json not on classpath");
            return parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** Score one row (length nFeatures). W is [out][in]. */
    public float score(float[] x, int offset) {
        float[] h = new float[nFeatures];
        for (int i = 0; i < nFeatures; i++) {
            float s = std[i] == 0f ? 1f : std[i];
            h[i] = (x[offset + i] - mean[i]) / s;
        }
        for (Layer l : layers) {
            float[] o = new float[l.b().length];
            for (int j = 0; j < o.length; j++) {
                float acc = l.b()[j];
                float[] wj = l.w()[j];
                for (int i = 0; i < h.length; i++) acc += wj[i] * h[i];
                o[j] = switch (l.activation()) {
                    case "relu" -> Math.max(0f, acc);
                    case "sigmoid" -> (float) (1.0 / (1.0 + Math.exp(-acc)));
                    case "tanh" -> (float) Math.tanh(acc);
                    default -> acc;
                };
            }
            h = o;
        }
        return h[0];
    }
}
