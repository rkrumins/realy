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
    /** Input names the model was trained on (weights JSON feature_names); empty for old files. */
    public final List<String> featureNames;
    private final float[] mean;
    private final float[] std;
    private final List<Layer> layers;
    /** Width of a request row, and where each model input sits in it (-1: absent, use the training mean). */
    private final int rowWidth;
    private final int[] index;

    private ProxyModel(String name, String version, int n, List<String> names, float[] mean, float[] std,
                       List<Layer> layers, int rowWidth, int[] index) {
        this.name = name;
        this.version = version;
        this.nFeatures = n;
        this.featureNames = List.copyOf(names);
        this.mean = mean;
        this.std = std;
        this.layers = layers;
        this.rowWidth = rowWidth;
        this.index = index;
    }

    /**
     * The same model reading rows laid out as {@code schemaNames} (the live feature schema): each input
     * is taken from the schema column of the same name, so a model trained before features were added
     * (or one that does not use them) scores the wider vector exactly as it scored the old one. An
     * input the schema no longer has reads its training mean (contributes nothing). Without names in
     * the weights file the layout is positional, as before.
     */
    public ProxyModel bind(List<String> schemaNames) {
        if (featureNames.isEmpty()) return new ProxyModel(name, version, nFeatures, featureNames, mean, std, layers,
                Math.max(nFeatures, schemaNames.size()), identity(nFeatures));
        var pos = new java.util.HashMap<String, Integer>();
        for (int i = 0; i < schemaNames.size(); i++) pos.put(schemaNames.get(i), i);
        int[] idx = new int[nFeatures];
        for (int i = 0; i < nFeatures; i++) idx[i] = pos.getOrDefault(featureNames.get(i), -1);
        return new ProxyModel(name, version, nFeatures, featureNames, mean, std, layers, schemaNames.size(), idx);
    }

    /** Values per request row this model expects. */
    public int rowWidth() {
        return rowWidth;
    }

    private static int[] identity(int n) {
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = i;
        return a;
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
            List<String> names = new ArrayList<>();
            for (JsonNode x : r.path("feature_names")) names.add(x.asText());
            if (!names.isEmpty() && names.size() != n) names.clear();
            return new ProxyModel(r.path("name").asText("fraud_mlp"), r.path("version").asText("0"), n, names,
                    floats(r.get("mean")), floats(r.get("std")), layers, n, identity(n));
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

    /** Score one row of {@link #rowWidth()} values, laid out as bound. W is [out][in]. */
    public float score(float[] x, int offset) {
        return score(x, offset, rowWidth);
    }

    /**
     * Score one row of {@code width} values: a row of the bound layout is read by name; a row exactly as
     * wide as the model's own inputs is read positionally (a client that sends the model's tensor).
     */
    public float score(float[] x, int offset, int width) {
        boolean bound = width == rowWidth;
        float[] h = new float[nFeatures];
        for (int i = 0; i < nFeatures; i++) {
            float s = std[i] == 0f ? 1f : std[i];
            int j = bound ? index[i] : (i < width ? i : -1);
            h[i] = j < 0 ? 0f : (x[offset + j] - mean[i]) / s;
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
