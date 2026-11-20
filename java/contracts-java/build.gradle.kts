import com.google.protobuf.gradle.id

plugins {
    alias(libs.plugins.protobuf)
}

dependencies {
    api(libs.protobuf.java)
    api(libs.protobuf.java.util)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    compileOnly(libs.javax.annotation)
}

sourceSets {
    main {
        proto { srcDir("../../contracts/proto") }
        // Ship the feature DSL + rule sets on the classpath so every service reads the same contract.
        resources {
            srcDir("build/generated-resources/contracts")
        }
    }
}

val copyContracts by tasks.registering(Copy::class) {
    from("../../contracts/features") { into("realy/contracts/features") }
    from("../../rules") { into("realy/rules") }
    from("../../models/proxy") { include("default.weights.json"); into("realy/models") }
    into(layout.buildDirectory.dir("generated-resources/contracts"))
}
tasks.named("processResources") { dependsOn(copyContracts) }

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}" }
    plugins {
        id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}" }
    }
    generateProtoTasks {
        all().forEach { it.plugins { id("grpc") } }
    }
}
