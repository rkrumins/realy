plugins {
    application
}
dependencies {
    implementation(project(":common"))
    implementation(libs.grpc.netty.shaded)
    runtimeOnly(libs.slf4j.simple)
}
application {
    mainClass.set("io.realy.emitter.LoadEmitterMain")
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+ZGenerational", "-Xms1g", "-Xmx4g")
}
