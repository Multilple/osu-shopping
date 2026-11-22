# Measurement run, not an entry: times candidate builds on generated data and
# exits with code 3 so the log comes back. Sources: probe-src/ and the branches
# of this repository; the pipeline is based on Team 67's Express Lane (MIT).
FROM scratch
COPY probe /probe
COPY generate /generate
COPY v /v
ENV PROBE_SIZES=60000000:32,20000000:31 PROBE_REPS=5 PROBE_LAPS=b,s2,s3 PROBE_DETAIL=none PROBE_BUDGET=154 PROBE_CHECKS=none PROBE_WX=none
USER 1000:1000
ENTRYPOINT ["/probe"]
