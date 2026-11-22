"""An example plugin aggregation, the Python twin of java/plugin-example (com.example.pulsebench.MinAggregation):
`agg: min`, the smallest value of a numeric field in the window.

Load it with REALY_AGGREGATION_PLUGINS=realy.plugin_example (verify, the console, the golden tools), or ship
your own module and name it there (or as a `pulsebench.aggregations` entry point of your package). Both
twins pass contracts/testdata/aggregations/min.json, written by

    python -m realy.features.aggregations conformance --plugin realy.plugin_example --agg min

See docs/dev/extending.md.
"""
from realy.features.aggregations import BucketAgg, register

MIN = register(BucketAgg("min", "numeric", merge=min, count_merge=False,
                         window=lambda s, a, b, folded, any_: folded if any_ else s.default))
