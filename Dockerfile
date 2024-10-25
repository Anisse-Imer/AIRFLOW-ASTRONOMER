FROM quay.io/astronomer/astro-runtime:12.2.0

USER root
RUN apt-get update && \
    apt-get -y install git
RUN git --version
RUN git config --global --add safe.directory /usr/local/airflow

WORKDIR /usr/local/airflow
# My github action is going to replace the GITHUBPAT value by one specified in the repo -- check the action file
ENV GITHUBPAT=<ACTIONGITHUBPAT>
RUN git config --global url."https://$GITHUBPAT@github.com/".insteadOf "https://github.com/"
RUN git submodule init 
RUN git submodule update --init --recursive

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
