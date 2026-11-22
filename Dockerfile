# Measurement run, not an entry: times candidate builds on generated data,
# checks that they write the same answer, and exits with code 3 so the log
# comes back. Sources: probe-src/ and the branches of this repository; the
# pipeline is based on Team 67's Express Lane (MIT).
FROM scratch
COPY probe /probe
COPY generate /generate
COPY v /v
ENV PROBE_SIZES=60000000:32 PROBE_REPS=3 PROBE_LAPS=s4 PROBE_DIGEST=s4,s4 PROBE_BUDGET=140 PROBE_DETAIL=none PROBE_CHECKS=none PROBE_WX=none
USER 1000:1000
ENTRYPOINT ["/probe"]
