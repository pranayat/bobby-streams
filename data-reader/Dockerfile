# Use an official Python runtime as a parent image
FROM python:3.9

WORKDIR /data-reader

COPY requirements.txt /data-reader

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run script when the container launches
CMD ["python3", "-m", "debugpy", "--listen", "0.0.0.0:5678", "--wait-for-client", "producer_script.py"]