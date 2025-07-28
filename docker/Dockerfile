# syntax=docker/dockerfile:1.6   # нужен BuildKit ≥ v0.11

FROM python:3.12-alpine AS builder

# Системные зависимости только для сборки wheels
RUN apk add --no-cache \
	build-base \
	gcc \
	musl-dev \
	linux-headers \
	libffi-dev \
	libmagic \
	file \
	unixodbc-dev \
	freetds-dev \
	freetds \
	curl


# Устанавливаем uv (один бинарь Rust)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
	&& install -Dm755 /root/.local/bin/uv  /usr/local/bin/uv \
	&& install -Dm755 /root/.local/bin/uvx /usr/local/bin/uvx \
	&& uv --version


WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT=/usr/local
# Копируем только метаданные — слой кэшируется отдельно
COPY ../pyproject.toml ../uv.lock ./

# Ставим **только runtime-зависимости** напрямую в system-site
# --system   → без виртуального окружения
# --group default  → не тянем dev-пакеты
RUN uv sync --locked --no-default-groups --no-editable


FROM python:3.12-alpine

# Минимум run-time libs (без toolchain)
RUN apk add --no-cache \
	libffi \
	libmagic \
	unixodbc-dev \
	file \
	libstdc++ \
	unixodbc \
	freetds \
	freetds-dev && \
	echo "[FreeTDS]\nDriver = /usr/lib/libtdsodbc.so\nSetup = /usr/lib/libtdsodbc.so\nUsageCount = 1" > /etc/odbcinst.ini && \
	odbcinst -q -d && ls -l /usr/lib/libtdsodbc.so



# Копируем установленный Python-стек из builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv

# Рабочая директория приложения

# Копируем только исходники проекта
COPY ../src /app/src
WORKDIR /app/src

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	PYTHONPATH=/app/src

