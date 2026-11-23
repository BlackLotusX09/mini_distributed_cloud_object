FROM node:22-alpine AS ui-builder
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
COPY controller/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY controller/*.py ./
COPY --from=ui-builder /frontend/dist /app/ui_dist
RUN mkdir -p /data
ENV PYTHONUNBUFFERED=1 DB_PATH=/data/controller.db FRONTEND_DIST=/app/ui_dist ADMIN_ENABLED=true
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=2)"
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
