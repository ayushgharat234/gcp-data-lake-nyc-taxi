# Use Python 3.10 as the base image
FROM python:3.10

# Set working directory to /app
WORKDIR /app

# Copy all files to /app
COPY . /app

# Install dependencies
RUN pip install -r requirements.txt  # Install dependencies

# Run the Flask app
CMD ["python", "main.py"]  