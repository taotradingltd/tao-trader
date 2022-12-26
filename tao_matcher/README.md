# gfm-vision

The purpose of this ```Dockerfile``` is to set up and run the ```gfm-vision``` app locally with minimal input required. The application - with all functionality - should run on ```localhost:8000```, if successful.

It uses a test database, which will be downloaded in the instruction set below. This database mirrors the schema and implementation of the production database, but does not contain any data within it.

## Instructions

Step 1: Make a backup of ```template.env```, then rename the original ```.env```. Fill in **all** the fields in this file. If you are not using Docker compose, then replace the ```DB_``` variables with their appropriate values. Please note that many of these values can be found in Keeper. Otherwise, log in to the corresponding service and generate an API key.

*NOTE: If you want to use Docker compose (recommended) to automate the full build of the project, then please skip the following steps and refer to the [README](../README.md) in the project root. However, some features do not currently work in the Docker compose build of the project.*

Step 2: Build the Docker image. In the directory of this project where the Dockerfile exists, run the following command:

```bash
docker build -t [image_name] .
```

You can optionally tag this image using a colon i.e. ```gfm-vision-docker:development```

Step 4: Start a Docker container with the built image:

```bash
docker run -d -p 8000:8000 [image_name]
```

*You can optionally name your container by providing the ```--name``` flag.*

Step 5: Access the application in your browser on ```http://localhost:8000```

### Notes

1. Please not that running this Docker container individually does not use [volumes](https://docs.docker.com/storage/volumes/). This means changes to the code will not be reflected inside the container.

2. Make sure you are on the ```dev``` branch for development:

```bash
git checkout dev
git pull origin dev
```
