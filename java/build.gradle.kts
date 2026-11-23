plugins {
    alias(libs.plugins.protobuf) apply false
    alias(libs.plugins.shadow) apply false
}

subprojects {
    apply(plugin = "java-library")
    group = "io.realy"
    version = "0.1.0"

    extensions.configure<JavaPluginExtension> {
        toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
    }
    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(21)
        options.compilerArgs.addAll(listOf("-Xlint:-options", "-parameters"))
    }
    val libs = rootProject.extensions.getByType<VersionCatalogsExtension>().named("libs")
    dependencies {
        "testImplementation"(platform(libs.findLibrary("junit-bom").get()))
        "testImplementation"(libs.findLibrary("junit-jupiter").get())
        "testImplementation"(libs.findLibrary("assertj").get())
        "testRuntimeOnly"(libs.findLibrary("junit-launcher").get())
    }
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        maxHeapSize = "2g"
        systemProperty("bench", System.getProperty("bench") ?: "false")
        systemProperty("golden.record", System.getProperty("golden.record") ?: "false")
        systemProperty("bench.out", System.getProperty("bench.out") ?: "")
        if (System.getProperty("bench") == "true") testLogging.showStandardStreams = true
        testLogging { events("failed"); exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL; showStandardStreams = false }
    }
}
