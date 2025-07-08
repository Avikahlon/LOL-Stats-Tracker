FROM python:3.13

WORKDIR /scraper

# Install cron
RUN apt-get update && apt-get install -y cron

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Add crontab file
COPY cronjob /etc/cron.d/scraper-cron

# Give execution rights
RUN chmod 0644 /etc/cron.d/scraper-cron \
    && crontab /etc/cron.d/scraper-cron

# Start cron
CMD ["cron", "-f"]