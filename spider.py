'''Reads urls from to_crawl.sqlite and dumps the raw html of the pages into
dump.sqlite'''

import sqlite3
import requests
from time import time

COMMIT_FREQ = 5

conn = sqlite3.connect('wsdump.sqlite')
cur = conn.cursor()

try:
    num_to_crawl = int(input("Crawl how many pages? (10) "))
except ValueError:
    num_to_crawl = 10

cur.execute('SELECT url FROM Pages ORDER BY crawled LIMIT ' + str(num_to_crawl))
rows = cur.fetchall()
rows.reverse()
fails = 0
crawled = 0
while ( len(rows) > 0 ):
    row = rows.pop()
    url = row[0]
    crawled += 1
    print(crawled, ': requesting', url)
    try:
        r = requests.get(url)
    except KeyboardInterrupt:
        print('')
        print('Aborted by user...')
        break

    if not r:
        print(f'Failed with response {r.statuscode} when getting', url)
        fails += 1
        if fails >= 5:
            print('Failed 5 in a row, aborting...')
            break
        else:
            continue
    
    cur.execute('''INSERT OR REPLACE INTO Pages (url, raw_html, crawled) 
                    VALUES (?, ?, ?)''', (url, r.text, int(time())))
    
    if crawled % COMMIT_FREQ == 0:
        conn.commit()

conn.commit()
conn.close()
