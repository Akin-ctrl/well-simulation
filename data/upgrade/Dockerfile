# Build a temporary tool image with both PostgreSQL versions for pg_upgrade.
# This image never runs an application service.
ARG OLD_IMAGE=timescale/timescaledb@sha256:0f5553d37abbe0f20bfe0a04dab5d1238955b5cda2711aba403090f95b6e51ff
ARG NEW_IMAGE=timescale/timescaledb@sha256:6f139d56042989bd35f50ba5986e492cd32e35cc6778c50be2659add298dc09f
FROM ${OLD_IMAGE} AS previous
FROM ${NEW_IMAGE}

COPY --from=previous /usr/local/ /opt/pg14/
# The older Alpine image uses older ICU libraries. Keep its dependencies private
# to the old binaries so the new server continues using its own libraries.
COPY --from=previous /usr/lib/ /opt/pg14-deps/
COPY old-binary.sh /opt/old-binary.sh
RUN chmod 755 /opt/old-binary.sh \
    && mkdir /opt/old-bin /upgrade \
    && chown postgres:postgres /upgrade \
    && for program in /opt/pg14/bin/*; do \
         ln -s /opt/old-binary.sh /opt/old-bin/${program##*/}; \
       done

USER postgres
WORKDIR /upgrade
ENTRYPOINT ["pg_upgrade"]
