import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pathlib import Path
import pickle
import gzip
import re
from urllib.parse import urlparse
for pt in Path().glob('./vars/htmls/*'):
    if 'url' not in pt.name:
        continue
    url = pt.open().read()
    if not re.match(r'https://youtube.com/watch', url):
        #print('not', url)
        continue


    pp = urlparse(url)    
    dd = dict([tuple(pair.split('=')) for pair in pp.query.split('&')])
    print(dd)
    html_pt = Path(str(pt).replace('.url', '') + '.html')
    html = gzip.decompress(html_pt.open('rb').read()).decode()
    soup = BeautifulSoup(html)
    print(soup.find('title'))
