# Set base image (host OS)
FROM nikolaik/python-nodejs:python3.8-nodejs16-slim

# RUN apt update && \
#     apt install -y curl

# ### Install npm
# ENV NODE_VERSION=16.10.0
# RUN curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.38.0/install.sh | bash

# ENV NVM_DIR=/root/.nvm
# RUN . "$NVM_DIR/nvm.sh" && nvm install ${NODE_VERSION}
# RUN . "$NVM_DIR/nvm.sh" && nvm use v${NODE_VERSION}
# RUN . "$NVM_DIR/nvm.sh" && nvm alias default v${NODE_VERSION}
# ENV PATH="/root/.nvm/versions/node/v${NODE_VERSION}/bin/:${PATH}"

# RUN npm install -g npm@7.24.0
RUN npm i -g @dataform/cli

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
