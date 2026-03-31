FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .
COPY vehicle_agent/ ./vehicle_agent/
CMD ["python", "-m", "vehicle_agent"]
