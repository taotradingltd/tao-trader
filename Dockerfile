FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE 1

RUN apt-get -y update && apt-get install -y cron netcat tree

COPY src/config/crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

WORKDIR /app

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY requirements.txt ./

COPY src .

RUN . /app/.env && pip install --no-cache-dir -r requirements.txt

EXPOSE 8888

ENTRYPOINT ["sh", "/docker-entrypoint.sh"]