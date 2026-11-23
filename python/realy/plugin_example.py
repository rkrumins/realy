"""An example plugin, the Python twin of java/plugin-example (com.example.pulsebench):
  - `agg: min`: the smallest value of a numeric field in the window (MinAggregation);
  - `fn: log_amount`: log(1 + amount in USD), a scoring-time derived function (LogAmountFn).

Load it with REALY_PLUGINS=realy.plugin_example (verify, the console, the golden tools), or ship your own
module and name it there (or as a `pulsebench.plugins` entry point of your package). Both twins pass
contracts/testdata/aggregations/min.json and contracts/testdata/derived/log_amount.json, written by

    python -m realy.features.aggregations conformance --plugin realy.plugin_example --agg min
    python -m realy.features.derived_fns conformance --plugin realy.plugin_example --fn log_amount

See docs/dev/extending.md.
"""
import math

from realy.features import derived_fns
from realy.features.aggregations import BucketAgg, register

MIN = register(BucketAgg("min", "numeric", merge=min, count_merge=False,
                         window=lambda s, a, b, folded, any_: folded if any_ else s.default))


def log_amount(event: dict, f: dict) -> float:
    return math.log1p(max(0, int(event.get("amount_usd_minor", 0))) / 100.0)


derived_fns.register("log_amount", log_amount)
