FROM python:3.12-slim
WORKDIR /app
COPY app.py config.py heartbeat.py ./
COPY storage ./storage
RUN mkdir -p /data
ENV PYTHONUNBUFFERED=1 PORT=8000 DATA_DIR=/data
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=5 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen('http://localhost:%s/health' % os.environ.get('PORT','8000'), timeout=2)"
CMD ["python", "app.py"]
