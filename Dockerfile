# Measurement run, not an entry: times candidate builds on generated data and
# exits with code 3 so the log comes back. Sources: probe-src/ and the branches
# of this repository; the pipeline is based on Team 67's Express Lane (MIT).
FROM scratch
COPY probe /probe
COPY generate /generate
COPY v /v
ENV PROBE_SIZES=60000000:32 PROBE_REPS=6 PROBE_LAPS=a,r1,r2,r3 PROBE_BUDGET=150 PROBE_DETAIL=none PROBE_CHECKS=none PROBE_WX=none
USER 1000:1000
ENTRYPOINT ["/probe"]
