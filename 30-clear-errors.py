import gzip
from bs4 import BeautifulSoup
import glob
from pathlib import Path
from tqdm import tqdm
for fn in tqdm(glob.glob('./vars/channels/*')):
    try:
        html = gzip.decompress(open(fn, 'rb').read())
        html = html.decode()
        title = BeautifulSoup(html).title.text
        if 'Just a moment...' in title:
            Path(fn).unlink()
        print(title)
    except Exception as exc:
        Path(fn).unlink()
        print(exc)


