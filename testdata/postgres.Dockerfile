FROM postgres:latest

ENV POSTGRES_PASSWORD password
ENV POSTGRES_DB bench

COPY tpch-postgres.sql /docker-entrypoint-initdb.d/