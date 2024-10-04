FROM python:3.12.7-slim

WORKDIR /tests

RUN apt-get update && apt-get install -y gcc python3-dev git

RUN git clone https://github.com/neptune-ai/neptune-api.git
RUN git clone https://github.com/neptune-ai/neptune-client-scale.git
RUN git clone -b dev/fetcher https://github.com/neptune-ai/neptune-client.git
RUN git clone https://github.com/neptune-ai/neptune-fetcher.git

COPY tests/requirements.txt /tests/
RUN pip install -U -r requirements.txt

COPY tests/run_tests.sh tests/*.py /tests/
CMD ["/bin/bash", "run_tests.sh"]
