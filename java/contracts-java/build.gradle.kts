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
            srcDir("build/generated-resources/descriptors")
        }
    }
}

val copyContracts by tasks.registering(Copy::class) {
    from("../../contracts/features") { into("realy/contracts/features") }
    // bindings: the entity registry, event types and pipelines (io.realy.scoring.binding)
    from("../../contracts") { include("entities.yaml", "events/**", "pipelines/**"); into("realy/contracts") }
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
        all().forEach {
            it.plugins { id("grpc") }
            // every contract message's descriptor on the classpath (realy/contracts/descriptors.binpb):
            // the generic codec decodes any bound message by name, generated class or not
            it.generateDescriptorSet = true
            it.descriptorSetOptions.includeImports = true
            it.descriptorSetOptions.path =
                "${layout.buildDirectory.get().asFile}/generated-resources/descriptors/realy/contracts/descriptors.binpb"
        }
    }
}
tasks.named("processResources") { dependsOn("generateProto") }
