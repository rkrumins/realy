plugins {
    application
}
dependencies {
    implementation(project(":scoring-core"))
    implementation(libs.armeria)
    implementation(libs.armeria.grpc)
    runtimeOnly(libs.slf4j.simple)
}
application {
    mainClass.set("io.realy.gateway.GatewayMain")
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+ZGenerational", "-Xms1g")
}
