# Contexto de build = raiz do projeto. Mantemos client/ e proto/ lado a lado
# em /build para que o caminho ../proto do pom.xml funcione também aqui.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY proto/ proto/
COPY client/pom.xml client/pom.xml
COPY client/src client/src
RUN mvn -f client/pom.xml -q package -DskipTests

# Imagem final: só o JRE e o jar (bem menor)
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/client/target/bot.jar .
CMD ["java", "-jar", "bot.jar"]
