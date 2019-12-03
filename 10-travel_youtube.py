from urllib.parse import urlencode, urlparse, urlunparse, parse_qs
import cgi
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
import multiprocessing

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

href = 'https://www.youtube.com/?gl=JP'

def url_parse(x):
    u = urlparse(x)
    query = parse_qs(u.query, keep_blank_values=True)
    for key in list(query.keys()):
        if key != 'v':
            query.pop(key, None)
    u = u._replace(query=urlencode(query, True))
    ret = urlunparse(u)
    # if ret != x:
    #    print(x, u, query, ret)
    return ret

def scan(arg):
    key, hrefs = arg
    Path('tmp/').mkdir(exist_ok=True)
    Path('vars/htmls').mkdir(exist_ok=True, parents=True)
    try:
        options = Options()
        options.add_argument('--headless')
        options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36")
        options.add_argument(f"user-data-dir=/tmp/work_{key:02d}")
        options.add_argument('lang=ja')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--window-size=1080,10800")
        driver = webdriver.Chrome(options=options,
                                  executable_path='/usr/bin/chromedriver')
    except Exception as exc:
        print(exc)
        return set()

    all_ret_hrefs = set()
    for href in hrefs:
        href = url_parse(href)
        print(1, href)
        try:
            if 'https://youtube.com/watch_videos' in href:
                continue
            if 'https://youtube.com/playlist' in href:
                continue
            if 'https://youtube.com/redirect' in href:
                continue
            if '.swf' in href or '.pdf' in href or '.zip' in href:
                continue
            if 'youtube.com' not in href:
                continue
            hashed = sha256(bytes(href, 'utf8')).hexdigest()[:16]
            if Path(f'vars/htmls/{hashed}.hrefs').exists():
                try:
                    all_ret_hrefs |= pickle.loads(gzip.decompress(
                        open(f'vars/htmls/{hashed}.hrefs', 'rb').read()))
                except EOFError:
                    Path(f'vars/htmls/{hashed}.hrefs').unlink()
                    continue
                #print(f'already fetched {href}')
                continue
            try:
                #r = requests.get(href, timeout=10)
                if '?' in href:
                    driver.get(href + '&gl=JP')

                else:
                    driver.get(href + '?gl=JP')
                time.sleep(5.0)
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight + 10000);")
                '''
                try:
                    driver.execute_script("window.scrollTo(0, document.querySelector('#contents').scrollHeight);")
                    TIMEOUT_IN_SECONDS = 10
                    wait = WebDriverWait(driver, TIMEOUT_IN_SECONDS)
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#contents")))
                except Exception as exc:
                    print(exc)
                '''
            except Exception as exc:
                # if 'Invalid URL' in exc.args[0]:
                print(exc)
                continue
            while True:
                html = driver.page_source
                soup = BeautifulSoup(html, features='lxml')
                if '読み込んでいます...' in soup.body.text:
                    time.sleep(2.5)
                else:
                    break
            try:
                print(href, soup.title.text, soup.body.text[:100])
            except:
                print('any error', href)
                continue
            # if 'YouTube' == soup.title.text:
            #    print(soup.body.text[:100])
            #print(soup.find('div', {'class':'ytd-item-section-renderer'}))

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


N = int(multiprocessing.cpu_count() * 1.4)


def chunking(hrefs):
    hrefs = list(hrefs)
    random.shuffle(hrefs)
    args = {}
    for idx, href in enumerate(hrefs):
        key = idx % N
        if key not in args:
            args[key] = []
        args[key].append(href)
    args = [(key, hrefs) for key, hrefs in args.items()]
    return args


hrefs = scan((1, [href]))
while True:
    hrefs_next = set()
    # for href in hrefs:
    #    hrefs_next |= scan(href)
    with PPE(max_workers=N) as exe:
        for hrefs_next_ in exe.map(scan, chunking(hrefs)):
            hrefs_next |= hrefs_next_
    hrefs = hrefs_next
