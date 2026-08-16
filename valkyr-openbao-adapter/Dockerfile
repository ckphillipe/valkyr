FROM rust:1.85-bookworm AS build
WORKDIR /source
COPY . .
RUN cargo build --release -p valkyr-openbao-adapter

FROM gcr.io/distroless/cc-debian13
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /source/target/release/valkyr-openbao-adapter /usr/local/bin/valkyr-openbao-adapter
ENTRYPOINT ["valkyr-openbao-adapter"]
CMD ["--config", "/etc/valkyr/adapter.yml"]
