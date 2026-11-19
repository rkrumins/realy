plugins {
    application
}
dependencies {
    implementation(project(":scoring-core"))
    runtimeOnly(libs.slf4j.simple)
    testImplementation(libs.grpc.inprocess)
}
application {
    mainClass.set("io.realy.model.MockModelServerMain")
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+ZGenerational", "-Xms512m")
}
