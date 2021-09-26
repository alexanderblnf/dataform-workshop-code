# Set base image (host OS)
FROM python:3.8-slim

RUN apt update && apt install -y git

# Set the working directory
WORKDIR .

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy over source files of the component
COPY /src /src
COPY main.py .

# Command to run on container start
ENTRYPOINT [ "python", "./main.py" ]
