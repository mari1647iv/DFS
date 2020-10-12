FROM python:3.7
COPY . /datanode
RUN useradd -ms /bin/bash datanode
RUN pip3 install --no-cache-dir -r /datanode/requirements.txt
ENTRYPOINT ["python", "/datanode/datanode.py"]