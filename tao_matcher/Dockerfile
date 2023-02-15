FROM python:3.10

ENV DJANGO_SETTINGS_MODULE "vision.settings.docker"
ENV PYTHONDONTWRITEBYTECODE 1

RUN apt-get -y update && apt-get install -y cron netcat wkhtmltopdf firefox-esr

COPY config/crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

WORKDIR /app

RUN mkdir -p /var/log/gunicorn/
RUN mkdir -p /var/run/gunicorn/
COPY config/gunicorn /config/gunicorn/

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY requirements.txt ./

COPY src .
COPY artifacts artifacts/

RUN . /app/.env && pip install --no-cache-dir -r requirements.txt
RUN python -c "import nltk;nltk.download('stopwords')"

# install geckodriver
RUN wget -O /tmp/geckodriver.tar.gz https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-linux64.tar.gz
RUN tar -xzf /tmp/geckodriver.tar.gz -C /usr/local/bin
RUN chmod +x /usr/local/bin/geckodriver

# set display port to avoid crash
ENV DISPLAY=:99

EXPOSE 8000

ENTRYPOINT ["sh", "/docker-entrypoint.sh"]