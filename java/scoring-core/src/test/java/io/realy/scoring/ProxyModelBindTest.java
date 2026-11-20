package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.scoring.model.ProxyModel;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProxyModelBindTest {
    // a two-input model: logit = 2*z(a) - 1*z(b); z = (x - mean) / std
    private static final String W = """
            {"name": "m", "version": "v", "n_features": 2, "feature_names": ["a", "b"],
             "mean": [1.0, 0.0], "std": [1.0, 2.0],
             "layers": [{"W": [[2.0, -1.0]], "b": [0.0], "activation": "sigmoid"}]}
            """;

    @Test
    void inputsAreReadByNameFromAWiderOrReorderedSchema() {
        ProxyModel m = ProxyModel.parse(W);
        float own = m.score(new float[] {3f, 4f}, 0);                       // a=3, b=4
        ProxyModel wide = m.bind(List.of("new1", "b", "x", "a", "new2"));
        assertThat(wide.rowWidth()).isEqualTo(5);
        assertThat(wide.score(new float[] {9f, 4f, 9f, 3f, 9f}, 0)).isEqualTo(own);   // same a, b: same score
        // a row of the model's own width is still read positionally
        assertThat(wide.score(new float[] {3f, 4f}, 0, 2)).isEqualTo(own);
        // an input the schema lacks reads its training mean: contributes nothing
        ProxyModel partial = m.bind(List.of("a", "other"));
        float aOnly = (float) (1.0 / (1.0 + Math.exp(-(2.0 * (3.0 - 1.0)))));
        assertThat(partial.score(new float[] {3f, 77f}, 0)).isEqualTo(aOnly);
    }
}
