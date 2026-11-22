# Extension plugins for the compose stack

Mounted read-only at `/opt/realy/plugins` (`PLUGINS_DIR`) in the gateway, the Flink JobManager and
TaskManagers, and the console.

- `*.jar`: Java extensions (plugin aggregations), loaded by `ServiceLoader` from
  `META-INF/services/io.realy.scoring.features.Aggregation`. The gateway and Flink must see the same jars.
- `python/<module>.py`: the Python twin of each extension, imported by the console and verify when named
  in `REALY_AGGREGATION_PLUGINS` (comma-separated module names).

Try the bundled example (`agg: min`):

```bash
gradle -p java :plugin-example:jar
cp java/plugin-example/build/libs/realy-plugin-example.jar deploy/compose/plugins/
REALY_AGGREGATION_PLUGINS=realy.plugin_example docker compose -f deploy/compose/docker-compose.yml up -d
pulsebench infra check     # the gateway reports the plugin; preflight checks its Python twin is loaded
```

Then add a feature that uses it to the schema. The full walkthrough is in `docs/dev/extending.md`.
