FROM python:3.10-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Let Python treat /app/api as root (for scrapers import)
ENV PYTHONPATH=/app/api

# Tell Uvicorn to run the correct file inside api/
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
