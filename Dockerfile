FROM python:3.14-slim

LABEL org.opencontainers.image.title="gpu-node-vsphere-maintenance-controller"
LABEL org.opencontainers.image.description="Kubernetes controller that automates ESXi maintenance mode for worker nodes with PCI passthrough (GPU or otherwise)."
LABEL org.opencontainers.image.source="https://github.com/Varashi/gpu-node-vsphere-maintenance-controller"
LABEL org.opencontainers.image.documentation="https://github.com/Varashi/gpu-node-vsphere-maintenance-controller/blob/main/README.md"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

RUN pip install --no-cache-dir pyVmomi==8.0.3.0.1 kubernetes==31.0.0

COPY controller.py .

CMD ["python", "-u", "controller.py"]
