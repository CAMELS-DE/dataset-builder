ARG PYTHON_VERSION=3.10

# use python base image
FROM python:${PYTHON_VERSION}
LABEL maintainer="Mirko Mälicke"

# build the structure for the python package
RUN mkdir -p /src/harvest
RUN mkdir -p /src/EZG
RUN mkdir -p /src/input_data
RUN mkdir -p /src/output_data
RUN touch /src/harvest/.incontainer

# copy sources
COPY ./harvest /src/harvest
COPY ./requirements.txt /src/requirements.txt
COPY ./setup.py /src/setup.py
COPY ./README.md /src/README.md
COPY ./LICENSE /src/LICENSE

# install python package
RUN pip install --upgrade pip
RUN pip install -e .

# create the entrypoint
WORKDIR /src/harvest
ENTRYPOINT ["python"]
CMD ["run.py"]