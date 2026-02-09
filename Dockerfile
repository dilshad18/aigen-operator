FROM python:3.11-alpine3.21

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Update all packages to latest versions (fixes CVE-2025-15467)
RUN apk upgrade --no-cache && \
    apk add --no-cache gcc musl-dev

# Create non-root user
RUN addgroup -S appuser && adduser -S appuser -G appuser

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chown -R appuser:appuser /app

USER appuser

CMD ["kopf", "run", "--standalone", "-A", "operator.py"]