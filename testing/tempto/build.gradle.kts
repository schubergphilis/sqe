plugins {
    application
}

repositories {
    mavenCentral()
    // trino-product-tests pulls Confluent-hosted kafka artifacts
    // (kafka-schema-registry-client, kafka-protobuf-*) for its Kafka tests.
    // Not used by the Iceberg suite, but they must resolve for the classpath.
    maven { url = uri("https://packages.confluent.io/maven/") }
}

dependencies {
    // Brings in tempto-core, trino-jdbc, and the iceberg product-test classes transitively.
    implementation("io.trino:trino-product-tests:465")
}

application {
    mainClass.set("io.trino.tests.product.TemptoProductTestRunner")
}

// Pass tempto args from the command line, e.g.:
//   gradle run --args="--help"
tasks.named<JavaExec>("run") {
    // Tempto writes reports/temp under the working dir; keep it inside the project.
    workingDir = layout.projectDirectory.asFile
}
