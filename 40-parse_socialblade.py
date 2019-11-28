from pathlib import Path
import gzip
from bs4 import BeautifulSoup
import re
from concurrent.futures import ProcessPoolExecutor as PPE

args = [pt for pt in Path().glob('./vars/channels/*')]

def parallel(arg):
    pt = arg
    for pt in [pt]:
        try:
            html = gzip.decompress(pt.open('rb').read()).decode('utf8')
            soup = BeautifulSoup(html)
            if soup.find('a', {'id':'youtube-user-page-country'}) is None:
                continue
            if soup.find('a', {'id':'youtube-user-page-country'}).text != 'JP':
                continue

            subs = soup.find('span', {'id':'youtube-stats-header-subs'}).text
            views = soup.find('span', {'id':'youtube-stats-header-views'}).text
            uploads = soup.find('span', {'id':'youtube-stats-header-uploads'}).text
            h1 = soup.find('h1').text

            estimate = None
            for d1 in soup.find_all('div', {'style':'float: left; width: 900px; height: 100px; margin-bottom: 10px;'}):
                for d2 in d1.find_all('div'):
                    if 'Estimated Monthly Earnings' in re.sub(r'\s{1,}', ' ', d2.text.strip()):
                        #print(d2.text.strip())
                        ma = re.search(r'\$(\d|\.|K|M){1,} - \$(\d|\.|K|M){1,}', d2.text.strip())
                        estimate = ma.group(0)
            
            ret = {'channel_id':pt.name, 
                'channel_name': h1,
                'subs':subs, 
                'views':views, 
                'uploads':uploads,
                'estimate':estimate }
            return ret 
        except Exception as exc:
            print(exc)
            return None
    return None

import pandas as pd
from tqdm import tqdm
recs = []
with PPE(max_workers=8) as exe:
    for rec in tqdm(exe.map(parallel, args), total=len(args)):
        if rec is None:
            continue
        else:
            print(rec)
            recs.append(rec) 
    #print(soup.find('a', {'id':'youtube-user-page-country'}).text)

df = pd.DataFrame(recs)
df.to_csv('channels.csv', index=None)
