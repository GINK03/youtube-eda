from concurrent.futures import ProcessPoolExecutor as PPE
from googleapiclient.discovery import build
import glob
from pathlib import Path
import os
import re
import pprint
import json
from tqdm import tqdm
import time
import requests
import gzip
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

pp = pprint.PrettyPrinter(indent=2)

#API_KEY = os.environ['ASTT_YOUTUBE_API_KEY']
#API_SERVICE_NAME = 'youtube'
#API_VERSION = 'v3'
#youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)
# e.g. query
# res = youtube.channels().list(
#       part = 'id,snippet,brandingSettings,contentDetails,invideoPromotion,statistics,topicDetails',
#        id = row.channelId
#   ).execute()


def parallel(arg):
    key, files = arg

    options = Options()
    options.add_argument('--headless')
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36")
    options.add_argument("user-data-dir=work")
    options.add_argument("--window-size=1080,10800")
    driver = webdriver.Chrome(options=options,
                              executable_path='/usr/bin/chromedriver')

    for pt in tqdm(files):
        print(pt)
        try:
            url = pt.open().read()
            if '/channel/' not in url:
                continue
            if not re.match(r'https://youtube.com/channel/[0-9a-zA-Z]{1,}$', url):
                continue

            channel_id = re.search(
                r'https://youtube.com/channel/([0-9a-zA-Z]{1,}$)', url).group(1)
            if Path(f'vars/channels/{channel_id}').exists():
                continue
            # pp.pprint(r)
            print(pt)
            driver.get(f'https://socialblade.com/youtube/channel/{channel_id}')
            # driver.get(f'https://socialblade.com/youtube/channel/mokouliszt')
            time.sleep(7.0)
            html = driver.page_source
            print(BeautifulSoup(html).title)
            with open(f'vars/channels/{channel_id}', 'wb') as fp:
                fp.write(gzip.compress(bytes(html, 'utf-8')))
        except Exception as exc:
            print(exc)
            time.sleep(10)


args = {}
for idx, pt in enumerate(Path().glob('./vars/htmls/*.url')):
    key = idx % 32
    if key not in args:
        args[key] = []
    args[key].append(pt)
args = [(key, pts) for key, pts in args.items()]
#[parallel(arg) for arg in args]
with PPE(max_workers=32) as exe:
    exe.map(parallel, args)
