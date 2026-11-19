FROM python:3.12.7-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Код монтується томом ./app:/app (див. docker-compose.*.yml), тому образ не треба
# перезбирати після кожної правки.
WORKDIR /app
