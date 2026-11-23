FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /uvx /bin/

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY fixtures ./fixtures

RUN uv sync --frozen --no-dev --no-editable

RUN mkdir -p /app/data /app/reports /app/assets

CMD ["uv", "run", "--no-sync", "exchange-collector", "demo"]
