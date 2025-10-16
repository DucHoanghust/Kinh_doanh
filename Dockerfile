FROM astrocrpublic.azurecr.io/runtime:3.0-8

# COPY instantclient_19_28 /opt/oracle/instantclient_19_28
# ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_19_28:${LD_LIBRARY_PATH}
USER root
# Cài unzip để giải nén
RUN apt-get update && \
    apt-get install -y unzip libaio1 && \
    rm -rf /var/lib/apt/lists/*

# Copy zip Linux Instant Client vào container
COPY instantclient-basic-linux.x64-19.28.0.0.0dbru.zip /opt/oracle/
RUN unzip /opt/oracle/instantclient-basic-linux.x64-19.28.0.0.0dbru.zip -d /opt/oracle/ \
    && rm /opt/oracle/instantclient-basic-linux.x64-19.28.0.0.0dbru.zip

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_19_28:${LD_LIBRARY_PATH}

# COPY dags/data/lat_long.csv /opt/airflow/dags/data/lat_long.csv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
USER astro