
import numpy as np
import pandas as pd
import random
try:
    import sys
    sys.path.append('./pytube/')
    from pytube import YouTube
except:
    ...
df = pd.read_csv('../vars/120_joined_filtered.csv')

def parallel(arg):
    vid = arg
    # print(vid)
    # return
    #yt = YouTube(f'https://www.youtube.com/watch?v={vid}')
    try:
        print(f'https://www.youtube.com/watch?v={vid}')
        yt = YouTube(f'https://www.youtube.com/watch?v={vid}')
    except Exception as exc:
        print(exc)
        return
    objs = []
    for stream in yt.streams.all():
        if 'video/mp4' in stream.mime_type and stream.resolution is not None:
            mime_type, resolution, filesize, stream = (
                stream.mime_type, stream.resolution, stream.filesize, stream)
            obj = {
                'mine_type': stream.mime_type,
                'resolution': resolution,
                'filesize': filesize,
                'stream': stream}
            print(obj)
            objs.append(obj)
    df = pd.DataFrame(objs).sort_values(by=['filesize'])
    # print(df)
    # use no.2
    df.iloc[1].stream.download(output_path='var', filename=f'{vid}')


vids = df.vid.tolist()
random.shuffle(vids)
for vid in vids:
    parallel(vid)
    #break
