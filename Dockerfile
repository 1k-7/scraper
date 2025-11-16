# Use an official, lightweight Python base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Create a directory for persistent data
# This is where novels.json and processed_pages.json will live
RUN mkdir /app/data

# Copy the requirements file first for better build caching
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Tell Docker that the /app/data directory should be a volume
VOLUME /app/data

# The command to run your bot when the container starts
CMD ["python", "scraper_bot.py"]
