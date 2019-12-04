import pandas as pd
import os
import shutil
from pathlib import Path
import json
BUCKET_NAME = 'gs://gimpei_tr16_2'
for pt in Path().glob('../video-downloads/var/*.mp4'):
    try:
        if Path(f'var/{pt.name.replace(".mp4", "")}.json').exists():
            continue
        print(pt.name)
        
        query1 = f'gsutil cp {pt} {BUCKET_NAME}/'
        query2 = f'gcloud ml video detect-labels {BUCKET_NAME}/{pt.name}'

        print(query1)
        print(query2)

        os.system(query1)

        res = os.popen(query2).read()
        obj = json.loads(res)

        with open(f'var/{pt.name.replace(".mp4", "")}.json', 'w') as fp:
            json.dump(obj, fp, indent=2)
    #break
    except Exception as exc:
        print(exc)
