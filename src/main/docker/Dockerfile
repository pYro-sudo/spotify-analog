FROM quay.io/quarkus/quarkus-distroless-image:2.0 as builder
WORKDIR /project
COPY . .
RUN ./mvnw package -Dquarkus.package.type=uber-jar

FROM quarkus/quarkus-distroless-image:2.0
COPY --from=builder /project/target/*-runner /app/application
EXPOSE 8080
CMD ["./application", "-Dquarkus.http.host=0.0.0.0"]