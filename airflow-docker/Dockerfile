FROM apache/airflow:3.0.1

USER root
# Install system dependencies for psycopg2 and other build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    python3-dev \
    libpq-dev \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy your Python dependencies file
COPY requirements.txt .

# Install dependencies once at build time
RUN pip install --no-cache-dir -r requirements.txt