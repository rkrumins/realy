plugins {
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":scoring-core")) {
        exclude(group = "org.slf4j")
    }
    implementation(libs.flink.connector.kafka)
    compileOnly(libs.flink.streaming.java)
    compileOnly(libs.flink.statebackend.rocksdb)
    compileOnly(libs.flink.connector.base)

    testImplementation(libs.flink.streaming.java)
    testImplementation(libs.flink.clients)
    testImplementation(libs.flink.statebackend.rocksdb)
    testImplementation(libs.flink.connector.base)
    testImplementation(libs.flink.test.utils)
    // the operator test harnesses (KeyedOneInputStreamOperatorTestHarness): state TTL in processing time
    testImplementation(variantOf(libs.flink.runtime) { classifier("tests") })
    testRuntimeOnly(libs.slf4j.simple)
}

tasks.shadowJar {
    archiveFileName.set("realy-flink-job.jar")
    mergeServiceFiles()
    manifest { attributes["Main-Class"] = "io.realy.flink.FraudJob" }
    // Flink provides its own logging and Scala/Flink runtime.
    exclude("org/slf4j/**", "META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
tasks.named("build") { dependsOn(tasks.shadowJar) }
