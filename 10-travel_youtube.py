import requests
from bs4 import BeautifulSoup
import re
from hashlib import sha256
import gzip
import pickle
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor as TPE
from concurrent.futures import ProcessPoolExecutor as PPE
import random
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

href = 'https://www.youtube.com/?gl=JP'


def scan(arg):
    key, hrefs = arg
    try:
        options = Options()
        options.add_argument('--headless')
        options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36")
        options.add_argument("user-data-dir=work")
        options.add_argument('lang=ja')
        options.add_argument("--window-size=1080,10800")
        driver = webdriver.Chrome(options=options,
                                  executable_path='/usr/bin/chromedriver')
    except Exception as exc:
        print(exc)
        return set()

    all_ret_hrefs = set()
    for href in hrefs:
        try:
            hashed = sha256(bytes(href, 'utf8')).hexdigest()[:16]
            if Path(f'vars/htmls/{hashed}.hrefs').exists():
                all_ret_hrefs |= pickle.loads(gzip.decompress(open(f'vars/htmls/{hashed}.hrefs', 'rb').read()))
                continue
            try:
                #r = requests.get(href, timeout=10)
                driver.get(href)
                time.sleep(2.0)
            except Exception as exc:
                # if 'Invalid URL' in exc.args[0]:
                print(exc)
                continue
            html = driver.page_source
            soup = BeautifulSoup(html, features='lxml')
            print(href, soup.title)

            with open(f'vars/htmls/{hashed}.html', 'wb') as fp:
                fp.write(gzip.compress(bytes(html, 'utf8')))
            with open(f'vars/htmls/{hashed}.url', 'w') as fp:
                fp.write(href)
            ret_hrefs = set()
            for a in soup.find_all('a', {'href': True}):
                ret_href = a.get('href')
                if re.match(r'^//', href):
                    ret_href = 'https:' + ret_href
                elif re.match(r'^/', ret_href):
                    ret_href = 'https://youtube.com' + ret_href
                else:
                    ...
                ret_hrefs.add(ret_href)
            with open(f'vars/htmls/{hashed}.hrefs', 'wb') as fp:
                fp.write(gzip.compress(pickle.dumps(ret_hrefs)))
            all_ret_hrefs |= ret_hrefs
        except Exception as exc:
            print(exc)
    driver.quit()
    return all_ret_hrefs
import random
def chunking(hrefs):
    hrefs = list(hrefs)
    random.shuffle(hrefs)
    args = {}
    for idx, href in enumerate(hrefs):
        key = idx%32
        if key not in args:
            args[key] = []
        args[key].append(href)
    args = [(key, hrefs) for key, hrefs in args.items()]
    return args

hrefs = scan((1, [href]))
#exit(1)
while True:
    hrefs_next = set()
    # for href in hrefs:
    #    hrefs_next |= scan(href)
    with PPE(max_workers=32) as exe:
        for hrefs_next_ in exe.map(scan, chunking(hrefs)):
            hrefs_next |= hrefs_next_
    hrefs = hrefs_next
