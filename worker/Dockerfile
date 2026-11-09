FROM python:3.12-slim
WORKDIR /app
COPY app.py ./
ENV PYTHONUNBUFFERED=1 PORT=8001
EXPOSE 8001
HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=5 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8001/health',timeout=2).status==200 else 1)"
CMD ["python", "app.py"]
