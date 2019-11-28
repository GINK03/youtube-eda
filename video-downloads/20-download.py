import numpy as np
import pandas as pd
from pytube import YouTube


yt = YouTube('https://www.youtube.com/watch?v=8odRUz22hTQ')

objs = []
for stream in yt.streams.all():
    if 'video/mp4' in stream.mime_type and stream.resolution is not None:
        mime_type, resolution, filesize, stream = (stream.mime_type, stream.resolution, stream.filesize, stream)
        obj = {
            'mine_type':stream.mime_type,
            'resolution':resolution,
            'filesize':filesize,
            'stream':stream}
        print(obj)
        objs.append(obj)

df = pd.DataFrame(objs).sort_values(by=['filesize'])
print(df)

# use no.2
df.iloc[1].stream.download(filename='any')
#yt.filter(progressive=True, file_extension='mp4')
