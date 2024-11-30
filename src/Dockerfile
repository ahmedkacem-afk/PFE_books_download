# Use an official Python runtime as the base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose a port if necessary (not needed for batch scripts)
EXPOSE 8080

# Command to run your script
CMD ["python", "scriptpfe.py"]
