# syntax=docker/dockerfile:1.7

FROM node:24.16.0-bookworm-slim@sha256:2c87ef9bd3c6a3bd4b472b4bec2ce9d16354b0c574f736c476489d09f560a203 AS frontend-builder
WORKDIR /src/frontend
RUN npm install --global pnpm@11.0.9
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY frontend/ ./
COPY scenarios/ /src/scenarios/
RUN pnpm build

FROM debian:trixie@sha256:f324c7ff54321e8d9c588493a20244965938ce0aa50bbd1022d38010e9ffc4b1 AS firmware-builder
ARG FIRMWARE_COMMIT=54e0d8d0ab2ff56b3a9ce967e53f79e49af560fb
ARG CLIENT_LIBRARY_VERSION=2.7.11
ARG UPSTREAM_BASE_IMAGE_DIGEST=sha256:23e92b1331a3a471eaef0c63cbca4365ca40b3111a9781cfdbe5a5114e5773d4
ARG MESHTASTICATOR_COMMIT=unavailable
ENV DEBIAN_FRONTEND=noninteractive
ENV PIP_ROOT_USER_ACTION=ignore
ENV PIP_BREAK_SYSTEM_PACKAGES=1
RUN apt-get update && apt-get install --no-install-recommends -y \
      ca-certificates curl g++ git libbluetooth-dev libgpiod-dev libi2c-dev \
      libinput-dev liborcania-dev libsdl2-dev libsqlite3-dev libssl-dev \
      libulfius-dev libusb-1.0-0-dev libuv1-dev libx11-dev \
      libxkbcommon-x11-dev libyaml-cpp-dev pkg-config python3-grpc-tools \
      python3-pip wget zip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m pip install --no-cache-dir platformio==6.1.19
WORKDIR /tmp/firmware
RUN git init \
    && git remote add origin https://github.com/meshtastic/firmware.git \
    && git fetch --depth=1 origin "${FIRMWARE_COMMIT}" \
    && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "${FIRMWARE_COMMIT}" \
    && git submodule update --init --recursive --depth=1
COPY docker/firmware-collision.patch /tmp/firmware-collision.patch
RUN git apply --check /tmp/firmware-collision.patch \
    && git apply /tmp/firmware-collision.patch \
    && grep -F -- '-DUSERPREFS_SIMRADIO_EMULATE_COLLISIONS=1' variants/native/portduino/platformio.ini \
    && bash ./bin/build-native.sh native \
    && cp "release/meshtasticd_linux_$(uname -m)" /tmp/meshtasticd \
    && strings /tmp/meshtasticd | grep -F 'Collision detected, dropping current and previous packet!'
RUN install -d /tmp/capability \
    && printf '%s\n' "firmware=${FIRMWARE_COMMIT}" "flag=USERPREFS_SIMRADIO_EMULATE_COLLISIONS=1" > /tmp/capability/native-collision-enabled
RUN patch_sha256="$(sha256sum /tmp/firmware-collision.patch | cut -d' ' -f1)" \
    && binary_sha256="$(sha256sum /tmp/meshtasticd | cut -d' ' -f1)" \
    && architecture="$(uname -m)" \
    && printf '{"firmwareCommit":"%s","collisionPatchSha256":"%s","firmwareBinarySha256":"%s","buildArchitecture":"%s","clientLibraryVersion":"%s","upstreamBaseImageDigest":"%s","meshtasticatorCommit":"%s"}\n' \
      "${FIRMWARE_COMMIT}" "${patch_sha256}" "${binary_sha256}" "${architecture}" \
      "${CLIENT_LIBRARY_VERSION}" "${UPSTREAM_BASE_IMAGE_DIGEST}" "${MESHTASTICATOR_COMMIT}" \
      > /tmp/capability/build-metadata.json

FROM python:3.13.7-slim-trixie@sha256:5f55cdf0c5d9dc1a415637a5ccc4a9e18663ad203673173b8cda8f8dcacef689 AS runtime
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV MESHTASTICD_BIN=/usr/bin/meshtasticd
ENV MESHTASTIC_LAB_DATA=/data
ENV MESHTASTIC_COLLISION_MARKER=/usr/share/meshtastic-lab/native-collision-enabled
ENV MESHTASTIC_BUILD_METADATA=/usr/share/meshtastic-lab/build-metadata.json
RUN apt-get update && apt-get install --no-install-recommends -y \
      libgpiod3 libi2c0 libinput10 liborcania2.3 libsdl2-2.0-0 libssl3t64 \
      libulfius2.7t64 libusb-1.0-0 libuv1t64 libx11-6 libxkbcommon-x11-0 \
      libyaml-cpp0.8 tini \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 lab \
    && useradd --uid 10001 --gid lab --home-dir /app --shell /usr/sbin/nologin lab \
    && install -d -o lab -g lab /app /data /usr/share/meshtastic-lab
WORKDIR /app
COPY requirements.lock ./
RUN python -m pip install --no-cache-dir --requirement requirements.lock
COPY backend/ ./backend/
COPY scenarios/ ./scenarios/
COPY --from=frontend-builder /src/frontend/dist ./frontend/dist/
COPY --from=firmware-builder /tmp/meshtasticd /usr/bin/meshtasticd
COPY --from=firmware-builder /tmp/capability/native-collision-enabled /usr/share/meshtastic-lab/native-collision-enabled
COPY --from=firmware-builder /tmp/capability/build-metadata.json /usr/share/meshtastic-lab/build-metadata.json
RUN chmod 0755 /usr/bin/meshtasticd \
    && /usr/bin/meshtasticd --help >/dev/null \
    && chown -R lab:lab /app /data
USER lab
VOLUME ["/data"]
EXPOSE 8080 45001 45002 45003 45004 45005 45006 45007 45008 45009 45010
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
  CMD ["python", "-c", "import json,urllib.request; d=json.load(urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=2)); assert d['status']=='ok'"]
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8080"]
