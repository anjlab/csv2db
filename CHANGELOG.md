Version 3.3.0
=============
- Upgrade build to Gradle 8.1.1
- Use [Nashorn OpenJDK](https://github.com/openjdk/nashorn) instead of Java's built-in Nashorn which is deprecated
- Upgrade Dockerfile to Java 17 (Amazon Corretto 17 on Amazon Linux 2023)
- Fix a regression in Java 11+ where the `emit` function was evaluated as `null`