# ---- Build stage ----
FROM python:3.11-alpine3.21 AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apk upgrade --no-cache && \
    apk add --no-cache gcc musl-dev

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- Runtime stage ----
FROM python:3.11-alpine3.21

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apk upgrade --no-cache

WORKDIR /app

COPY --from=builder /install /usr/local

RUN addgroup -S appuser && adduser -S appuser -G appuser

COPY operator.py .
RUN chown -R appuser:appuser /app

USER appuser

ENV OPERATOR_NAMESPACE="whiz-operator"

ENTRYPOINT ["/bin/sh", "-c", "kopf run --standalone --namespace=$OPERATOR_NAMESPACE operator.py"]
