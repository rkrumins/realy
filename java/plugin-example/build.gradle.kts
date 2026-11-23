// An example extension jar: the `min` aggregation (docs/dev/extending.md). Build it with
// `gradle -p java :plugin-example:jar`, put build/libs/realy-plugin-example.jar in PLUGINS_DIR for the
// gateway and Flink, and set REALY_PLUGINS=realy.plugin_example for the Python twin.
dependencies {
    compileOnly(project(":scoring-core"))
    testImplementation(project(":scoring-core"))
}
tasks.jar { archiveFileName.set("realy-plugin-example.jar") }
