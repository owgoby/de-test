FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
COPY requirements-test.txt .
RUN pip install --no-cache-dir -r requirements.txt -r requirements-test.txt
# Код монтируется томом из ./solution, чтобы правки не требовали пересборки образа.
COPY . .
ENTRYPOINT ["python", "-m", "pipeline"]
CMD ["run"]
