plugins {
    application
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("org.apache.flink", "flink-clients", "1.18-SNAPSHOT")
    implementation("org.apache.flink", "flink-state-processor-api", "1.18-SNAPSHOT")

    runtimeOnly("org.apache.logging.log4j:log4j-core:2.20.0")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
}

application {
    mainClass.set("demo.app.Main")
}

tasks.compileJava {
    options.isDeprecation = true
    options.compilerArgs.add("-Xlint:unchecked")
}

tasks.jar {
    manifest {
        attributes("Main-Class" to application.mainClass.get())
    }
}
