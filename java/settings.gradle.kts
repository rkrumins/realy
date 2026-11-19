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

// MAVEN_MIRROR (env or -PmavenMirror=...): a Maven Central mirror tried first — for corporate
// repositories, air-gapped Artifactory/Nexus, or to avoid Maven Central rate limits (e.g.
// https://maven-central.storage-download.googleapis.com/maven2).
val mavenMirror: String? = (System.getenv("MAVEN_MIRROR") ?: providers.gradleProperty("mavenMirror").orNull)
    ?.takeIf { it.isNotBlank() }

dependencyResolutionManagement {
    repositories {
        mavenMirror?.let { m -> maven { url = uri(m) } }
        maven { url = uri("https://repo1.maven.org/maven2") }
        mavenCentral()
    }
}
pluginManagement {
    val mirror: String? = (System.getenv("MAVEN_MIRROR") ?: providers.gradleProperty("mavenMirror").orNull)
        ?.takeIf { it.isNotBlank() }
    repositories {
        mirror?.let { m -> maven { url = uri(m) } }
        gradlePluginPortal()
        maven { url = uri("https://repo1.maven.org/maven2") }
    }
}
