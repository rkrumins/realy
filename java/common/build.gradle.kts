dependencies {
    api(project(":contracts-java"))
    api(libs.kafka.clients)
    api(libs.jackson.databind)
    api(libs.jackson.yaml)
    api(libs.micrometer.prometheus)
    api(libs.hdrhistogram)
    api(libs.slf4j.api)
}
