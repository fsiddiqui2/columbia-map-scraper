# 1. Start with the Python version from your logs
FROM python:3.13-slim

# 2. Install system dependencies (Chromium and its driver)
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    --no-install-recommends \
 && rm -rf /var/lib/apt/lists/*

# 3. Set up the working directory
WORKDIR /opt/render/project/src

# 4. Install your Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy all your project code into the container
COPY . .