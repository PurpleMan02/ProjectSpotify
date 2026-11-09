plugins {
    id("java")
    application
    // Builds one runnable "fat" jar that Airflow calls: java -jar mamramic-etl.jar <command>
    id("com.gradleup.shadow") version "9.1.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

tasks.shadowJar {
    archiveFileName = "mamramic-etl.jar"
    destinationDirectory = file("../jars")
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
    mergeServiceFiles()
}

val jacksonVersion = "2.20.0"

dependencies {
    implementation("com.rometools:rome:2.1.0")

    implementation("com.rometools:rome-modules:2.1.0")

    implementation("io.minio:minio:8.5.17")

    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")

    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml:${jacksonVersion}")

    implementation("org.jsoup:jsoup:1.21.2")

    implementation("org.duckdb:duckdb_jdbc:1.4.1.0")

    implementation("org.postgresql:postgresql:42.7.8")

    implementation("info.picocli:picocli:4.7.7")

    implementation("org.slf4j:slf4j-simple:2.0.17")

    // Tests
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "DataHandler"
}


tasks.test {
    useJUnitPlatform()
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}

tasks.withType<JavaExec>().configureEach {
    jvmArgs("-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
}
