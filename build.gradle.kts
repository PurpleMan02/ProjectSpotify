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

val jacksonVersion = "2.20.0"

dependencies {
    // MinIO (S3) buckets: bronze / silver uploads, bucket creation
    implementation("io.minio:minio:8.5.17")

    // RSS feed.xml parsing (handles itunes:* tags)
    implementation("com.rometools:rome:2.1.0")

    // JSON interaction events + Java time types
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    // Strip HTML from episode descriptions
    implementation("org.jsoup:jsoup:1.21.2")

    // Cleaning with SQL + writing Parquet straight to MinIO
    implementation("org.duckdb:duckdb_jdbc:1.4.1.0")

    // Gold layer: PostgreSQL
    implementation("org.postgresql:postgresql:42.7.8")

    // Command-line subcommands (bronze, silver-episodes, gold, ...)
    implementation("info.picocli:picocli:4.7.7")

    // Logging (quiet SLF4J warnings from libraries)
    implementation("org.slf4j:slf4j-simple:2.0.17")

    // Tests
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    // Change this if your main class lives somewhere else
    mainClass = "Main"
}

tasks.shadowJar {
    archiveFileName = "mamramic-etl.jar"
    mergeServiceFiles()
}

tasks.test {
    useJUnitPlatform()
}
