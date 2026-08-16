FROM rust:1.85-bookworm AS build
WORKDIR /source
COPY . .
RUN cargo build --release -p valkyr-db-adapter

FROM gcr.io/distroless/cc-debian13
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /source/target/release/valkyr-db-adapter /usr/local/bin/valkyr-db-adapter
ENTRYPOINT ["valkyr-db-adapter"]
CMD ["--config", "/etc/valkyr/adapter.yml"]
