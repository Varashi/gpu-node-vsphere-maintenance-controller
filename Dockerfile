FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pyVmomi==8.0.3.0.1 kubernetes==31.0.0

COPY controller.py .

CMD ["python", "-u", "controller.py"]
