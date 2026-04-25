# Dockerfile for emerging-edge multi-user deploy on Fly.io.
# Builds a self-contained image that runs `monitor.py serve` with
# MULTI_USER=1 and a persistent volume mounted at /data.

FROM python:3.14-slim

# pypdf is the only third-party dep (fund-letter PDF extraction).
# Everything else is stdlib.
RUN pip install --no-cache-dir pypdf==6.10.2

WORKDIR /app

# Copy source. We deliberately DON'T copy emerging_edge.db — each
# user gets their own DB at /data/u_<id>.db at signup time.
COPY *.py /app/
COPY *.json /app/
COPY logos /app/logos
COPY *.jpeg /app/

# Vikingship logo is optional; ignore if missing
RUN test -f /app/vikingship.jpeg || echo "no logo"

ENV MULTI_USER=1
ENV EE_DATA_DIR=/data
ENV PORT=8080

# Fly.io healthcheck pings /healthz
EXPOSE 8080

CMD ["python3", "monitor.py", "serve", "--port", "8080"]
