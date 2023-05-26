#!/bin/sh
while ! nc -z db 3306; do
    echo "MySQL is unavailable - sleeping"
    sleep 2
done

echo "MySQL is up - executing command...."
python manage.py makemigrations
python manage.py migrate

# FIX: cron doesn't start properly
echo "Starting cron service...."
service cron start
service cron restart

echo "Creating default superuser...."
echo "from django.contrib.auth.models import User;User.objects.create_superuser(username='admin', password='pass');" | python manage.py shell 2> /dev/null

# If you would like the articles table to automatically populate, uncomment the
# following two lines:
# echo "Running initial daily jobs in background...."
# python manage.py runjobs daily &

exec "$@"