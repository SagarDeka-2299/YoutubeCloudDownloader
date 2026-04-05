FROM python:3.12-slim

# Install ffmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml .

RUN uv pip install --system --no-cache \
    fastapi \
    "uvicorn[standard]" \
    "yt-dlp>=2025.1.1" \
    python-multipart \
    aiofiles && \
    # Always upgrade yt-dlp to absolute latest after base install
    pip install --upgrade --no-cache-dir yt-dlp

# Application code
COPY main.py .
COPY db.py   .
COPY static/ static/

# Volume mount points (actual data lives on Docker volumes)
RUN mkdir -p /app/data /app/audio /app/video

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
