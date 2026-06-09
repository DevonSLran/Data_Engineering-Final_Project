# Airflow image for the Olist pipeline.
# dbt is installed in an ISOLATED venv so its pins never fight Airflow's
# constraint set. The DAG calls it via the absolute path /opt/dbt-venv/bin/dbt.
FROM apache/airflow:3.0.1

# --- dbt in its own venv (keeps dbt-core deps away from Airflow) ---
# /opt is root-owned and the base image runs as the unprivileged `airflow`
# user, so build the venv as root and hand it to the airflow user/group.
# Install from a fully-pinned lockfile (dbt-requirements.txt) so the entire
# transitive tree is reproducible — pinning only dbt-core/dbt-postgres would
# let sub-deps drift and could re-introduce a pre-release dbt-core.
USER root
COPY dbt-requirements.txt /dbt-requirements.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt \
    && chown -R airflow:0 /opt/dbt-venv

# Pre-create the dir that holds the SimpleAuthManager password file, owned by
# airflow:0 and group-writable. A fresh named volume mounted here inherits this
# ownership, so the init container (running as an arbitrary AIRFLOW_UID in group
# 0) can write the seeded admin password — otherwise the mountpoint is root-only.
RUN mkdir -p /opt/airflow/generated \
    && chown airflow:0 /opt/airflow/generated \
    && chmod 0775 /opt/airflow/generated
USER airflow

# --- light deps for the bronze ingestion script (into Airflow's env) ---
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
