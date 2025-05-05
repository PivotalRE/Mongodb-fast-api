# Use an official Python base image
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

# Copy all files into the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port
EXPOSE 8000

# Run the FastAPI app
CMD ["uvicorn", "pvl.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
