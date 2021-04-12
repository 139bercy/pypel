FROM python:3.9.1-slim

WORKDIR /app
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

# Copy the module in /app
COPY pypel ./pypel

# Copy our main function as well
COPY docker/main.py .

CMD [ "python3", "main.py", "/data/*"]
