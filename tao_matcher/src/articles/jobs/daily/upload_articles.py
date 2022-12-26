import datetime
import email
import imaplib
import newspaper
import os
import urllib.parse

import pandas as pd
import _pickle as pickle

from base64 import b64decode
from tqdm import tqdm

from django.conf import settings
from django.db import OperationalError
from django_extensions.management.jobs import DailyJob

from articles import meltwater, models

class Job(DailyJob):
    help = "Upload daily Meltwater articles."

    def execute(self):
        search_ids = [
            17203327,       # MASTER
        ]

        for _id in search_ids:
            meltwater.meltwater_query(search_id=_id)

        search_ids_with_ml = [
            17203418,       # Mainstream sources
        ]

        for _id in search_ids_with_ml:
            meltwater.meltwater_query(search_id=_id, ml_filter=True)

        username = os.environ["MB_USER"]
        password = os.environ["MB_PASS"]
        imap_server = os.environ["IMAP"]

        mail = imaplib.IMAP4_SSL(imap_server)
        mail.login(username, password)
        mail.select('inbox')

        EXCLUSION_LIST = [
            "google",
            "hedgeweek",
            "privateequitywire",
        ]

        _, data = mail.search(None, f'(SINCE "{(datetime.date.today()-datetime.timedelta(days=1)).strftime("%d-%b-%Y")}")')

        mail_ids = []

        for block in data:
            mail_ids += block.split()

        settings.LOGGER.info(f"Found {len(mail_ids)} emails - getting content")

        messages = []

        for i in tqdm(mail_ids, desc="Scraping emails..."):
            _, data = mail.fetch(i, '(RFC822)')

            for response_part in data:
                if isinstance(response_part, tuple):
                    message = email.message_from_bytes(response_part[1])

                    _ = message['from']
                    _ = message['subject']

                    if message.is_multipart():
                        mail_content = ''
                        for part in message.get_payload():
                            if part.get_content_type() == 'text/plain':
                                mail_content += part.get_payload()
                    else:
                        mail_content = message.get_payload()

                    try:
                        content = b64decode(mail_content).decode("utf-8-sig", errors="ignore").replace("\r\n", "\n")
                    except Exception as e:
                        continue
                    content_split = [x.strip() for x in content.split(">\n\n")]

                    messages.extend(content_split)

        for i, message in enumerate(messages):
            if "\n\n" in message:
                messages[i] = message.split("\n\n")[1]

        articles = []

        settings.LOGGER.info(f"Scraping {len(messages)} articles")

        for doc in tqdm(messages, desc="Newspaper3k scraping..."):
            try:
                doc, url = doc.split("<http")
            except ValueError as e:
                continue
            url = "http" + url.split()[0]

            if url:
                if url.startswith("<"):
                    url = url[1:]
                if url.endswith(">"):
                    url = url[:-1]

                url = urllib.parse.urlparse(url)
                url = urllib.parse.parse_qs(url.query)

                if url.get("url"):
                    if len(url.get("url")):
                        url = url["url"][0]
                    else:
                        continue
                else:
                    continue

                if any('.' + exc + '.' in url for exc in EXCLUSION_LIST):
                    settings.LOGGER.info(f"Skipping: {url}")
                    continue

            npa = newspaper.Article(url=url)
            npa.download()
            try:
                npa.parse()
            except newspaper.ArticleException:
                settings.LOGGER.info(f"Newspaper3k failure: {url}")
                continue

            try:
                publish_date = datetime.datetime.strptime(str(npa.publish_date).split("+")[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            except:
                publish_date = datetime.date.today().strftime("%Y-%m-%d")

            articles.append({
                "url": url,
                "title": npa.title,
                "content": '<p>' + npa.text.replace('\n', '</p><p>') + '</p>',
                "date_published": publish_date
            })

        with open(settings.MODELS_DIR / 'article_selection.pkl', 'rb') as f:
            model = pickle.load(f, fix_imports=True)

        df = pd.DataFrame(articles)
        df = df[ df["content"] != "<p></p>" ]
        df['prediction'] = model.predict(df['title'].fillna("").to_list())

        settings.LOGGER.info(f"{df.shape[0]} articles founds; {df[df['prediction']==1].shape[0]} useful; excluding rest")

        df = df[df["prediction"]==1]

        for _, row in df.iterrows():
            title = row['title']
            content = row['content']
            url = row['url']
            publish_date = row['date_published']

            if url.endswith("/"):
                url = url[:-1]
            if not url.startswith("http"):
                url = "https://" + url
            if url.startswith("http://"):
                url = url.replace("http://", "https://")

            try:
                a = models.Article(
                    title=title,
                    content=content,
                    url=url,
                    publish_date=publish_date,
                    editorial_title=models.Title.NONE.value,
                    kind="Google Alerts",
                )
                a.save()
            except OperationalError:
                settings.LOGGER.warning(f"Failed to insert {url}")
                continue
