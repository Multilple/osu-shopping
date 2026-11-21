# Measurement run (not an entry)

Times candidate builds of the pipeline on generated 20M and 60M event datasets
and exits with code 3 so the log is returned. Nothing here is meant to score.

- `probe`: the runner, source in `probe-src/`.
- `generate`: the published generator, built unchanged.
- `v/`: candidate builds of the pipeline on the `express` branch family of this
  repository, which is based on Team 67's Express Lane
  (https://github.com/jason-scheffel/express-lane, MIT License).
