# Use Python 3.11 as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file first (for better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python script
COPY worldbank_scraper.py .


ENV PYTHONUNBUFFERED=1


# Run the script when container starts
CMD ["python", "worldbank_scraper.py"]