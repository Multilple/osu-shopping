# Measurement build (prebuilt): times runs on generated data and reports on stderr. Not an entry.
FROM scratch
COPY ss /ss
COPY ss2 /ss2
COPY ss3 /ss3
COPY ss4 /ss4
COPY gen /gen
USER 1000:1000
CMD ["/ss"]
