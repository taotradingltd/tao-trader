# TODO: this entire module should be a worker node, and shouldn't be executed
#       on the main server! Also, module warrants it's own refactoring...

import datetime
import email
import imaplib
import newspaper
import os
import pickle
import pytz

import pandas as pd

from bs4 import BeautifulSoup, SoupStrainer
from requests import get
from time import sleep, time, strftime
from urllib.parse import urlsplit

from selenium.common.exceptions import TimeoutException
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from django.db import OperationalError
from django.db.models import Count
from django.conf import settings

from .models import Article, Title

TZ = pytz.timezone('Europe/London')

def get_webdriver():
    options = FirefoxOptions()
    options.headless = True
    settings.LOGGER.debug("Initializing browser in headless mode." if options.headless else "Initializing browser in view.")

    browser = webdriver.Firefox(options=options)

    # set general timeout of browser to 5 seconds, but if forcing a wait then wait for 15 seconds
    browser.implicitly_wait(5)
    wait = WebDriverWait(browser, 15)

    if browser and wait:
        settings.LOGGER.info("Browser initialized.")
        return browser, wait
    settings.LOGGER.error("Failed to initialize browser.")

def meltwater_login(browser, wait):
    url = "https://app.meltwater.com"
    browser.get(url)

    # Log in to Meltwater
    wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "mw-app-login__email"))).send_keys(os.environ["MELTWATER_USER"])
    wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "mw-app-login__login-button"))).click()
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="password"]'))).send_keys(os.environ["MELTWATER_PW"])
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Log in')]"))).click()

    settings.LOGGER.info("Successfully logged into Meltwater.")
    sleep(5)

def get_dj_articles(browser, wait, article_urls):
    browser.get("https://app.meltwater.com/explore/advanced")
    actions = ActionChains(browser)

    text = dict()
    max_retries = 5
    for i in range(0, len(article_urls), 20):
        if not max_retries:
            continue

        t = [x for x in article_urls[i:i+20] if x not in text]

        query = ' OR '.join([f'URL:"{url}"' for url in t])

        # Refresh page to reset search box as unable to send keys to the search box
        browser.refresh()
        try:
            browser.implicitly_wait(5)
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".mw-search__input-wrapper"))).click()
            actions.send_keys(query).perform()
            actions.key_down(Keys.CONTROL).key_down(Keys.ENTER).key_up(Keys.CONTROL).key_up(Keys.ENTER).perform()
            wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "mw-content-document-title")))
        except:
            settings.LOGGER.critical("Failed to load page.")
            browser.get("https://app.meltwater.com/explore/advanced")
            max_retries -= 1
            sleep(5)
            continue

        elems = browser.find_elements(By.XPATH, "//mw-media-document")

        for elem in elems:
            try:
                x = elem.find_element(By.CLASS_NAME, "mw-content-document-title")
                url = x.get_attribute("href").replace("unsafe:", '')
                x.click()
                wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "mw-hosted-content__modal__section")))
                article_text = browser.find_element(By.CLASS_NAME, "mw-hosted-content__modal__section").text
                article_text = '<p>' + article_text.replace('\n', '</p><p>') + '</p>'
                article_text = article_text.replace('<p></p>', '')
                text[url] = article_text
                settings.LOGGER.debug(' '.join(text[url].split(' ')[:10]) + '...')
                sleep(2)
                wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "mw-mini-modal__cancel-button"))).click()
                sleep(1)
            except:
                settings.LOGGER.warning(f"Did not get text for article: {url}")
                try:
                    wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "mw-mini-modal__cancel-button"))).click()
                except:
                    pass
                sleep(2)
        if any(_ not in text for _ in t):
            i -= 20
            max_retries -= 1
        else:
            max_retries = 5
    return text

def get_csv_link(submitted, check_every=10, max_check=600):
    assert(submitted is not None)
    start = time()
    settings.LOGGER.info(f"Waiting for email to be received.")

    username = os.environ["MS_USER"]
    password = os.environ["MS_PW"]
    mailbox = os.environ["MS_MAILBOX"]
    imap_server = os.environ["IMAP"]

    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(f"{username}\\\{mailbox}", password)
    mail.select('inbox')

    message = None

    while time() < start + max_check:
        sleep(check_every)

        messages = []
        try:
            _, data = mail.search(None, "ALL")

            for d in data:
                messages += d.decode("utf-8").split()
        except IndexError:
            continue

        _, content = mail.fetch(messages[-1], '(RFC822)')

        if isinstance(content[0], tuple):
            message = email.message_from_bytes(content[0][1])

        settings.LOGGER.info(message.keys())

        sender = message.get("From").strip().lower()
        settings.LOGGER.info(sender)

        # Tue, 4 Oct 2022 13:13:29 +0000
        received_s = message.get("Date").strip()
        received = datetime.datetime.strptime(
            received_s, "%a, %d %b %Y %H:%M:%S %z"
        ).astimezone(TZ)
        settings.LOGGER.info(received)
        settings.LOGGER.info(submitted)

        message = message.get_payload().replace("=\r\n", "").replace("=3D", "=")

        if received > submitted and "meltwater" in sender:
            settings.LOGGER.info("Received new email from Meltwater.")
            break

    if not len(message):
        settings.LOGGER.error("No email received from Meltwater.")
        return

    bs = BeautifulSoup(message, 'html.parser', parse_only=SoupStrainer('a'))

    download_link = None
    for link in bs:
        if link.has_attr('href') and "amazonaws" in link["href"]:
            download_link = link["href"]
            settings.LOGGER.info(f"Found aws download link in email: {download_link}")
            break

    if download_link:
        return download_link
    settings.LOGGER.error("Did not find aws download link in email from Meltwater.")
    return

def download_csv(download_link):
    assert(download_link is not None)
    filename = settings.TMP_DIR / f"meltwater-{strftime('%Y%m%d-%H%M%S')}.csv"
    settings.LOGGER.debug(f"Saving to {filename}")
    try:
        csv = get(download_link)
        csv.encoding = csv.apparent_encoding
        settings.LOGGER.info("Successfully downloaded csv.")
    except:
        settings.LOGGER.error("Failed to download csv.")
        return

    with open(filename, 'w+') as f:
        f.write(csv.text)
        settings.LOGGER.info(f"CSV written succesfully to {filename}")
        return filename

def our_source_login(browser, wait):
    # Financial Times
    settings.LOGGER.info("Logging into Financial Times.")
    browser.get("https://ft.com/login")
    wait.until(EC.element_to_be_clickable((By.ID, "enter-email"))).send_keys(os.environ["FT_USER"])
    wait.until(EC.element_to_be_clickable((By.ID, "enter-email-next"))).click()
    wait.until(EC.element_to_be_clickable((By.ID, 'enter-password'))).send_keys(os.environ["FT_PW"])
    wait.until(EC.element_to_be_clickable((By.ID, "sign-in-button"))).click()
    sleep(5)

    # Bloomberg (CAPTCHA'D)
    # settings.LOGGER.info("Logging into Bloomberg.")
    # browser.get("https://www.bloomberg.com/account/signin?in_source=login_direct")
    # sleep(3)

    # # accept cookies
    # iframe = browser.find_element(By.XPATH, "//iframe[@id='sp_message_iframe_549774']")
    # browser.switch_to.frame(iframe)
    # wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Accept')]"))).click()

    # browser.switch_to.default_content()
    # wait.until(EC.element_to_be_clickable((By.ID, "form-element-email"))).send_keys(os.environ["BLOOMBERG_USER"])
    # wait.until(EC.element_to_be_clickable((By.ID, 'form-element-password'))).send_keys(os.environ["BLOOMBERG_PW"])
    # browser.find_element(By.XPATH, "/html/body/div[2]/div/div/main/div/div[3]/form/div[3]/button").click()
    # sleep(5)

def predefined_search(browser, wait, search_id):
    assert(search_id)
    settings.LOGGER.debug(search_id)
    browser.get(f"https://app.meltwater.com/explore/overview?searchId={search_id}")

    sleep(10)
    submitted = datetime.datetime.now().astimezone(TZ)
    browser.execute_script('angular.element(document.querySelectorAll(\'flux-list-item[ng-click="$ctrl.onAllExport()"]\'))[0].click();')
    settings.LOGGER.info("Results download requested as csv.")
    settings.LOGGER.debug(submitted)
    sleep(5)
    return submitted

def get_full_article_text(csv: pd.DataFrame, browser=None, wait=None):
    if not browser or not wait:
        browser, wait = get_webdriver()
        meltwater_login(browser, wait)
        sleep(2)

    our_sources = ["\.ft\."]
    meltwater_sources = ["nytimes"]

    dow_jones = csv[csv["URL"].str[:3] == "djp"]
    others = csv[csv["URL"].str.contains('|'.join(meltwater_sources))]

    can_get_meltwater = pd.concat([dow_jones, others])

    del dow_jones, others

    normals = csv[csv["URL"].fillna('').str.contains("http")]

    can_get_external = normals.copy(deep=True)
    can_get_external["domain"] = can_get_external["URL"].apply(lambda x: urlsplit(x).netloc)
    can_get_external = can_get_external[can_get_external["domain"].str.contains('|'.join(our_sources))]
    can_get_external.drop(["domain"], axis=1, inplace=True)

    confirmed_extractable = pd.concat([can_get_meltwater, can_get_external])

    potentially_extractable = pd.concat([csv, confirmed_extractable, confirmed_extractable]).drop_duplicates(keep=False)

    del normals, confirmed_extractable

    settings.LOGGER.info(f"{can_get_meltwater.shape[0]} available in Meltwater.")
    settings.LOGGER.info(f"{can_get_external.shape[0]} available externally behind paywalls.")
    settings.LOGGER.info(f"{potentially_extractable.shape[0]} potentially available.")

    settings.LOGGER.info("Getting full text for Meltwater articles.")

    text = get_dj_articles(browser, wait, can_get_meltwater["URL"].to_list())

    if text:
        for article in text:
            can_get_meltwater.loc[can_get_meltwater["URL"]==article, "Opening Text"] = text[article]

    for i, article in potentially_extractable.iterrows():
        npa = newspaper.Article(url=article["URL"])
        npa.download()
        try:
            npa.parse()
        except newspaper.ArticleException:
            settings.LOGGER.warning(f"Failed for article: {article['URL']}")
            continue

        if len(npa.text.strip().split()) >= 20:
            potentially_extractable.at[i, "Opening Text"] = npa.text

    csv = pd.concat([can_get_external, can_get_meltwater, potentially_extractable])
    del can_get_meltwater, can_get_external, potentially_extractable

    return csv

def meltwater_query(search_id=None, path=None, ml_filter=False):
    assert(search_id or path)
    browser, wait = get_webdriver()

    try:
        meltwater_login(browser, wait)
        sleep(2.5)

        try:
            wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "sm-close"))).click()
            settings.LOGGER.info("Closed rate dialogue.")
        except TimeoutException:
            settings.LOGGER.warning("Rate dialogue not found.")

        if not path:
            submitted = predefined_search(browser, wait, search_id)

            link = get_csv_link(submitted)
            path = download_csv(link)

        if path:
            try:
                csv = pd.read_csv(path, sep='\t')
            except UnicodeDecodeError:
                settings.LOGGER.warning("Failed to decode in UTF8, trying UTF16.")
                csv = pd.read_csv(path, encoding="UTF-16", sep='\t')
            except:
                settings.LOGGER.error("Failed to open csv.")
                return
            finally:
                os.remove(path)

            try:
                keep = ["Date", "Headline", "URL", "Opening Text", "Source"]
                csv = csv[keep]
            except:
                settings.LOGGER.error('CSV does not contain proper columns. Ensure (["Date", "Headline", "URL", "Opening Text", "Source"] in csv.columms == True)')
                return
            csv["Date"] = csv["Date"].apply(lambda x: datetime.datetime.strptime(x.split()[0], "%d-%b-%Y"))

            settings.LOGGER.info("Removing duplicate articles across Headline or URL.")
            csv = csv.drop_duplicates(subset=['URL'])
            csv = csv.drop_duplicates(subset=['Headline'])
            csv['URL'].fillna('', inplace=True)

            urls = [url['url'] for url in Article.objects.all().values('url')]
            csv = csv[~csv['URL'].isin(urls)]

            if ml_filter:
                with open(settings.MODELS_DIR / 'article_selection.pkl', 'rb') as f:
                    model = pickle.load(f, fix_imports=True)

                # Use model to determine relevance
                csv['Useful'] = model.predict(csv['Headline'].fillna("").to_list())
                csv = csv[csv['Useful'] == 1]

            csv = get_full_article_text(csv, browser, wait)
            csv['Opening Text'].fillna('', inplace=True)

            # Upload csv to db
            settings.LOGGER.info('Saving articles to database.')
            df_records = csv.to_dict('records')

            for record in df_records:
                try:
                    a = Article(
                        title=record['Headline'],
                        content=record['Opening Text'],
                        url=record['URL'],
                        source=record['Source'],
                        publish_date=record['Date'],
                        editorial_title=Title.NONE.value,
                        kind="Meltwater",
                    )
                    a.save()
                except OperationalError:
                    settings.LOGGER.warning(f"Failed to insert {record['Headline']}")
                    continue

            duplicate_urls = [x["url"] for x in Article.objects.values('url').annotate(url_count=Count('url')).filter(url_count__gt=1)]

            for url in duplicate_urls:
                articles = Article.objects.filter(url=url).order_by('date_added')

                for article in articles[1:]:
                    article.delete()
    except Exception as e:
        settings.LOGGER.error(str(e))
    finally:
        settings.LOGGER.info("Quitting browser.")
        browser.quit()
