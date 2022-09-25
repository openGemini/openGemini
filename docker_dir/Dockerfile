FROM registry-cbu.huawei.com/influxdb_fuxi_x86/euleros:2.2.5

RUN mkdir -p /home/pyxisdb/bin
RUN mkdir -p /home/pyxisdb/config
RUN mkdir -p /home/pyxisdb/scripts

COPY bin/ts-* /home/pyxisdb/bin/
COPY config/*.conf /home/pyxisdb/config/
COPY config/module.* /home/pyxisdb/config/
COPY scripts/*.sh /home/pyxisdb/scripts/

RUN chmod +x /home/pyxisdb/bin/*
RUN chmod +x /home/pyxisdb/scripts/*

RUN sed -i '/^%paas/{/ALL$/d}' /etc/sudoers

ENTRYPOINT ["/home/pyxisdb/scripts/start.sh"]
