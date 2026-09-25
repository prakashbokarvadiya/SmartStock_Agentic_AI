# ============================================================
# SmartStock Agentic AI — Dockerfile
# Multi-stage build: smaller final image
# ============================================================

# ---------- Stage 1: build dependencies ----------
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps needed to compile psycopg2 / other C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python deps into a prefix (easy to copy later)
COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt


# ---------- Stage 2: lean runtime image ----------
FROM python:3.11-slim AS runtime

LABEL maintainer="SmartStock Team"
LABEL description="SmartStock Agentic AI — FastAPI + LangGraph + MCP + Supabase"

# Runtime system libs only (libpq for psycopg2 driver)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash appuser

# Set working directory
WORKDIR /app

# Copy application source
COPY --chown=appuser:appuser app.py .
COPY --chown=appuser:appuser mcp_server.py .
COPY --chown=appuser:appuser email_automate.py .
COPY --chown=appuser:appuser requirements.txt .

# Copy templates (Jinja2 HTML files)
COPY --chown=appuser:appuser templates/ ./templates/

# Copy schema (used for DB init reference)
COPY --chown=appuser:appuser schema.sql .

# Copy Gmail OAuth credentials if they exist
# (token.pickle is generated at runtime via OAuth flow)
COPY --chown=appuser:appuser credentials.json* ./

# Create directories for persistent data
RUN mkdir -p /app/chat_memory && chown appuser:appuser /app/chat_memory

# Switch to non-root user
USER appuser

# ── Environment defaults (overridden by docker-compose / --env-file) ──
# Never hard-code real secrets here — use .env file or docker secret
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Health check — calls /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose the FastAPI port
EXPOSE 8000

# ── Entrypoint: production-grade uvicorn (no --reload in prod) ──
CMD ["uvicorn", "app:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info", \
     "--timeout-keep-alive", "30"]
