# Use an official Python runtime as the base image
FROM continuumio/miniconda3:latest

# Set the working directory inside the container
WORKDIR /app

# Install build essentials including GCC
RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get clean

# Copy the environment.yml file to the container
COPY environment.yml .

# Create a conda environment
RUN conda env create -f environment.yml

# Activate the conda environment
RUN echo "conda activate astra" >> ~/.bashrc
ENV PATH /opt/conda/envs/astra/bin:$PATH

# Copy the whole application to the container but ignore the files in .dockerignore
COPY . .

# Expose the port your FastAPI app runs on
EXPOSE 8000

# Set the working directory inside the container src
WORKDIR /app/code/src

# Command to run your FastAPI app
CMD ["python", "main.py", "--truncate"]
