FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY proto/ proto/
COPY client/pom.xml client/pom.xml
COPY client/src client/src
RUN mvn -f client/pom.xml -q package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/client/target/bot.jar .
CMD ["java", "-jar", "bot.jar"]
