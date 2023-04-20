FROM ubuntu:18.04

RUN apt-get update && apt-get install -y net-tools

RUN mkdir -p /opt/openGemini /var/log/openGemini/ \
    && chmod ugo+Xrw -R /opt/openGemini \
    && chmod ugo+Xrw -R /var/log/openGemini


COPY config/openGemini.singlenode.conf /opt/openGemini/
COPY build/ts-server /usr/bin/
COPY build/ts-cli /usr/bin/
COPY docker/server/entrypoint.sh /entrypoint.sh


RUN chmod +x /usr/bin/ts-server \
    && chmod +x /usr/bin/ts-cli \
    && chmod +x /entrypoint.sh

EXPOSE 8086

VOLUME /opt/openGemini
VOLUME /var/log/openGemini

ENV OPENGEMINI_CONFIG /opt/openGemini/openGemini.singlenode.conf

ENTRYPOINT ["/entrypoint.sh"]