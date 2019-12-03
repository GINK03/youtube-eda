import os
import sys

read = os.popen("curl -I -s 'https://www.youtube.com/watch?v=QmyhcjpsF6E'").read()
print(read)

head = read.split('\n')[0]

if '429' in head:
    print('使いすぎでバンされています')
if '200' in head:
    print('正常')
