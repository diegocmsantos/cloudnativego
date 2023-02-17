FROM scratch

COPY kvs .

COPY cert.pem .
COPY key.pem .

EXPOSE 8080

CMD ["/kvs"]