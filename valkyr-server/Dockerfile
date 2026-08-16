FROM rust:1.85-bookworm AS build
WORKDIR /source
COPY . .
RUN cargo build --release -p valkyr-server

FROM gcr.io/distroless/cc-debian13
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /source/target/release/valkyr-server /usr/local/bin/valkyr-server
EXPOSE 8080 8081 8090
ENTRYPOINT ["valkyr-server"]
CMD ["--config", "/etc/valkyr/server.yml"]
