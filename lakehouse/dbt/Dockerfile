# dbt runner: dbt-core + dbt-trino on Python slim (tag verified with `docker manifest inspect`).
FROM python:3.12.16-slim-bookworm

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

# The project is bind-mounted here by compose, so `dbt docs generate` leaves target/ on the host.
WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app
ENTRYPOINT ["dbt"]
CMD ["build"]
