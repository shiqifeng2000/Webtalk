# ARG DEBIAN_FRONTEND=noninteractive
# ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility,video
# From ubuntu:18.04 as ffnc
From ubuntu:noble as ffnc
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# WORKDIR /
COPY ./deploy/sources.list /etc/apt/sources.list
RUN apt-get update; exit 0
RUN apt-get install -y curl libssl-dev openssl 

WORKDIR /app
# RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# RUN /bin/bash -c "source '$HOME/.cargo/env'"
# COPY ./deploy/config-global /root/.cargo/config

COPY --from=webtalk:builder /app/webtalk /app/webtalk
COPY --from=webtalk:builder /app/log4rs.yaml /app/log4rs.yaml

CMD ["/app/webtalk"]