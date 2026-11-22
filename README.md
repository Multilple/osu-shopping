# osu-shopping

Team Wix's entry for the Databricks Shopping Sessions track of the Builders Buildathon 2026.

It reads shopping events, ad clicks and linked user IDs from `/data` and writes the
`user_daily` and `campaign_daily` Parquet tables under `/output`.

## Credit

This entry is built on **Team 67's Express Lane**
(https://github.com/jason-scheffel/express-lane), commit
`1cda738c004b3b186b878bdb8eee6516bfa9f22a`, released under the MIT License.
The Rust sources in `src/`, the build settings in `.cargo/` and the Dockerfile
come from that project. Changes made by Team Wix are listed in `CHANGES.md`.
See `THIRD_PARTY_NOTICES.md` and `LICENSE`.

## Build and run

Needs a Linux x86-64 host with Cascade Lake class AVX-512. IDs are treated as opaque strings (scorer version 5).

```sh
docker build --platform linux/amd64 -t osu-shopping .
docker run --rm --network none -v "$PWD/data:/data:ro" -v "$PWD/output:/output" osu-shopping
```
