# Shopping Sessions (Builders Buildathon, Databricks track)

One static Rust binary, no dependencies. Implements the track's rules.md.

    docker build -t shopping-sessions .

Stages, each on every core: link IDs (lock-free union-find in a shared ID table),
scan and parse events and clicks once, drop duplicate events, then per person:
sort, sessionize, attribute, aggregate, and write Parquet.
