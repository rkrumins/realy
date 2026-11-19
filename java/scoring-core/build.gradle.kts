dependencies {
    api(project(":common"))
    api(libs.cel)
    api(libs.lettuce)
    api(libs.grpc.netty.shaded)
    testImplementation(libs.grpc.inprocess)
    testRuntimeOnly(libs.slf4j.simple)
}
