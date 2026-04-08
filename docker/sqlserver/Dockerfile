# Pin to the exact CU that produced the data files.
# Bump MSSQL_TAG and re-run scripts/publish-sqlserver-image.sh to upgrade.
ARG MSSQL_TAG=2022-CU23-ubuntu-22.04
FROM mcr.microsoft.com/mssql/server:${MSSQL_TAG}
COPY --chown=mssql:root data/ /var/opt/mssql/data/
