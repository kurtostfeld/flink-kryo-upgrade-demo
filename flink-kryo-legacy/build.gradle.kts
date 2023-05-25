plugins {
    application
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.flink", "flink-streaming-java", "1.17.0")
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
