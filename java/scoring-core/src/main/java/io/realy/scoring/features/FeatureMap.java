package io.realy.scoring.features;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/** Zero-copy {@code map<string,double>} view of a feature vector for CEL ({@code f.<name>}). */
public final class FeatureMap extends AbstractMap<String, Double> {
    private final FeatureSchema schema;
    private final float[] values;

    public FeatureMap(FeatureSchema schema, float[] values) {
        this.schema = schema;
        this.values = values;
    }

    @Override
    public Double get(Object key) {
        if (!(key instanceof String s)) return null;
        try {
            return (double) values[schema.indexOf(s)];
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Set<Entry<String, Double>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<String, Double>> iterator() {
                return new Iterator<>() {
                    int i = 0;

                    @Override
                    public boolean hasNext() {
                        return i < values.length;
                    }

                    @Override
                    public Entry<String, Double> next() {
                        int j = i++;
                        return Map.entry(schema.features.get(j).name(), (double) values[j]);
                    }
                };
            }

            @Override
            public int size() {
                return values.length;
            }
        };
    }
}
