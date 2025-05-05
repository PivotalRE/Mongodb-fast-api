# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy everything
COPY . .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r DbAutomation/requirements.txt

# Add the root directory to PYTHONPATH
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8000

# Run the FastAPI app
CMD ["uvicorn", "DbAutomation.api.main:app", "--host", "0.0.0.0", "--port", "8000"]