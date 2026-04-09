FROM python:3.12-slim

# Install ffmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

# Install exact versions from lockfile into the system Python
RUN UV_SYSTEM_PYTHON=1 uv sync --frozen --no-cache

# Application code
COPY main.py .
COPY db.py   .
COPY static/ static/

# Volume mount points (actual data lives on Docker volumes)
RUN mkdir -p /app/data /app/audio /app/video

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
