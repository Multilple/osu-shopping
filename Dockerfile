# Measurement build (prebuilt): times runs on generated data and reports on stderr. Not an entry.
FROM scratch
COPY ss /ss
COPY gen /gen
USER 1000:1000
CMD ["/ss"]
