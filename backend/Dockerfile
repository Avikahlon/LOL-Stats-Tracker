# Use an official Python image
FROM python:3.13

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the backend code
COPY . .

# Expose port (change if needed)
EXPOSE 5000

# Run the app
CMD ["uvicorn", "back:app", "--host", "0.0.0.0", "--port", "8000"]
