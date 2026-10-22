### Security

- Distrusted emSign intermediates are no longer accepted in TLS certificate checks

    `rustls-webpki` 0.103.16 (RUSTSEC-2026-0382), `webpki-roots` 1.0.10
    (RUSTSEC-2026-0381) and `webpki-root-certs` 1.0.10 (RUSTSEC-2026-0383)
    reject certificate paths through the emSign
    intermediates whose private keys are now public. The dbt-sqe adapter's
    lock takes `certifi` 2026.10.19 (GHSA-6q48-6c7w-9gw2).
