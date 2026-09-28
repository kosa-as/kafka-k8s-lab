FROM quay.io/strimzi/kafka:1.2.0-kafka-4.3.1
WORKDIR /app
COPY app.py /app/app.py
USER 65532:65532
EXPOSE 8443
ENTRYPOINT ["/usr/bin/python3", "/app/app.py"]
