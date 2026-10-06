FROM postgres:18-alpine
RUN apk add --no-cache coreutils
COPY backup.sh /usr/local/bin/backup
RUN chmod 755 /usr/local/bin/backup
ENTRYPOINT ["backup"]
