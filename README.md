# Shopping Sessions (Builders Buildathon, Databricks track)

One static Rust binary, no dependencies. `rules.md` is the spec it implements.

    docker build -t shopping-sessions .

Stages, each on every core: link IDs (lock-free union-find in a shared ID table),
scan and parse events and clicks once, drop duplicate events, then per person:
sort, sessionize, attribute, aggregate, and write Parquet.
