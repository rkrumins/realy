rootProject.name = "realy"

include(
    "contracts-java",
    "common",
    "scoring-core",
    "mock-model-server",
    "scoring-gateway",
    "flink-job",
    "load-emitter",
)

dependencyResolutionManagement {
    repositories {
        maven { url = uri("https://repo1.maven.org/maven2") }
        mavenCentral()
    }
}
pluginManagement {
    repositories { gradlePluginPortal(); maven { url = uri("https://repo1.maven.org/maven2") } }
}
