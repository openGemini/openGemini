FROM openeuler/openeuler

RUN mkdir -p /opt/openGemini/bin
RUN mkdir -p /opt/openGemini/scripts

COPY bin/ts-* /opt/openGemini/bin/
COPY scripts/*.sh /opt/openGemini/scripts/

RUN chmod +x /opt/openGemini/bin/*
RUN chmod +x /opt/openGemini/scripts/*

ENTRYPOINT ["/opt/openGemini/scripts/start.sh"]
