FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies (no project itself, just deps)
RUN uv sync --frozen --no-install-project --no-dev

# Copy application code
COPY src/stream-manager.py .

# Use uv's managed venv to run the app
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8002

CMD ["uvicorn", "stream-manager:app", "--host", "0.0.0.0", "--port", "8002"]