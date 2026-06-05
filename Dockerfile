FROM python:3.13-slim

LABEL org.opencontainers.image.title="vsphere-passthrough-node-controller"
LABEL org.opencontainers.image.description="Kubernetes controller that automates ESXi maintenance mode for worker nodes with PCI passthrough (GPU or otherwise)."
LABEL org.opencontainers.image.source="https://github.com/Varashi/vsphere-passthrough-node-controller"
LABEL org.opencontainers.image.documentation="https://github.com/Varashi/vsphere-passthrough-node-controller/blob/main/README.md"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

RUN pip install --no-cache-dir --disable-pip-version-check \
      pyVmomi==9.1.0.0 kubernetes==36.0.1

COPY controller.py fence.py ./

# Default entrypoint = maintenance controller. The fence controller (fence.py)
# is the same image with the command overridden to `python -u fence.py`.
CMD ["python", "-u", "controller.py"]
