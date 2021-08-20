'''Downloads pages and enters new links to be crawled'''

import sqlite3, wikipedia
from time import time

def get_more_rows(cur, max_to_fetch):
    cur.execute(f'SELECT title, from_id FROM To_Crawl ORDER BY added LIMIT {max_to_fetch}')
    rows = []
    more_rows = cur.fetchall()
    if more_rows is not None:
        rows += more_rows
    if len(rows) < max_to_fetch:
        cur.execute(f'SELECT title, NULL FROM Pages ORDER BY crawled LIMIT {max_to_fetch - len(rows)}')
        more_rows = cur.fetchall()
        if more_rows is not None:
            rows += more_rows
    if len(rows) == 0:
        raise Exception("No rows to fetch!")
    return rows

COMMIT_FREQ = 5

conn = sqlite3.connect('wsindex.sqlite')
cur = conn.cursor()

try:
    num_to_crawl = int(input("Crawl how many pages? (10) "))
except ValueError:
    num_to_crawl = 10

rows = get_more_rows(cur, num_to_crawl)

crawled = 0
while crawled < num_to_crawl:
    if len(rows) == 0:
        rows = get_more_rows(cur, num_to_crawl - crawled)
    row = rows.pop()

    print(row)
    # crawl page here

    crawled += 1
    if crawled % COMMIT_FREQ == 0:
        conn.commit()

conn.commit()
conn.close()
