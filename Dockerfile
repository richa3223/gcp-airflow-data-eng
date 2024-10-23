FROM python:3.11-slim

# Define location to store the pipeline artifacts
ARG WORKDIR=/template
RUN mkdir -p ${WORKDIR}/modules

# Set working directory to /template
WORKDIR ${WORKDIR}

# Copy pipeline and package modules to working directory
COPY test-pipeline.py .
COPY modules modules
COPY requirements.txt .
COPY pyproject.toml .
COPY __init__.py .
COPY README.rst .

# Set configuration env var for main pipeline file to run
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/test-pipeline.py"

ENV PYTHONPATH=${WORKDIR}

# Install python dependencies. Use `--no-cache-dir` to reduce image size
RUN apt-get update
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir apache-beam[gcp]==2.59.0
RUN pip install --no-cache-dir -r requirements.txt
RUN pip check

# Install the pipeline package on the container
RUN pip install -e .

# Install launcher binary from Flex Template Launcher base image
COPY --from=gcr.io/dataflow-templates-base/python311-template-launcher-base:20240812-rc01 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Install entrypoint script from Apache Beam SDK base image
COPY --from=apache/beam_python3.11_sdk:2.59.0 /opt/apache/beam/boot /opt/apache/beam/boot

# Set the entrypoint such that this can act as an Apache Beam SDK container
ENTRYPOINT ["/opt/apache/beam/boot"]