FROM apache/superset:latest

USER root

# Install PostgreSQL driver into the Superset virtual environment
RUN pip install --no-cache-dir --target /app/.venv/lib/python3.10/site-packages psycopg2-binary

USER superset
