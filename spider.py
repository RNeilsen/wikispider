'''Downloads pages and enters new links to be crawled'''

import sqlite3, wikipedia
from time import time

MAX_CONSEC_FAILS = 5
COMMIT_FREQ = 2
wikipedia.set_rate_limiting(True)

def get_more_rows(cur, max_to_fetch):
    cur.execute(f'SELECT NULL, title, from_id FROM To_Crawl ORDER BY added LIMIT {max_to_fetch}')
    rows = []
    more_rows = cur.fetchall()
    if more_rows is None:
        print("Warning: no rows found in To_Crawl table")
    else:
        rows += more_rows
    if len(rows) < max_to_fetch:
        cur.execute(f'SELECT page_id, title, NULL FROM Pages ORDER BY crawled LIMIT {max_to_fetch - len(rows)}')
        more_rows = cur.fetchall()
        if more_rows is not None:
            rows += more_rows
    if len(rows) == 0:
        raise Exception("No rows to fetch!")
    return rows


conn = sqlite3.connect('wsindex.sqlite')
cur = conn.cursor()

try:
    num_to_crawl = int(input("Crawl how many pages? (10) "))
except ValueError:
    num_to_crawl = 10

rows = get_more_rows(cur, num_to_crawl)

crawled = 0
fails = 0
while crawled < num_to_crawl:
    if fails >= MAX_CONSEC_FAILS:
        print(f'{fails} failures in a row, terminating...')
        break

    if len(rows) == 0:
        rows = get_more_rows(cur, num_to_crawl - crawled)
    (page_id, title, from_id) = rows.pop()

    if page_id is not None:
        wp = wikipedia.page(pageid=page_id, preload=True)
    else:
        try:
            wp = wikipedia.page(title, preload=True, auto_suggest=False)
        except wikipedia.exceptions.PageError:
            print(f'Could not find title "{title}", replacing in To_Crawl...')
            fails += 1
            cur.execute('''DELETE FROM To_Crawl WHERE title=?''', (title,))
            cur.execute('''INSERT OR REPLACE INTO To_Crawl 
                    (title, added, from_id) VALUES (?,?,?)''' , 
                    (title, int(time()), from_id) )
            continue
    
    print('Page found:', wp)

    links = wp.links
    cur.execute('''INSERT OR REPLACE INTO Pages 
            (page_id, title, raw_text, crawled) VALUES (?, ?, ?, ?)''',
            (wp.pageid, wp.title, wp.content, int(time())) )
    cur.execute(f'''DELETE FROM To_Crawl WHERE 
            title=? or title=? ''', (title, wp.title))
    if from_id is not None:
        cur.execute(f'''INSERT INTO Links (from_id, to_id) VALUES (?,?)''',
                (from_id, wp.pageid) )
    for link in links:
        cur.execute('''SELECT page_id FROM Pages WHERE title=?''', (link,))
        found_link = cur.fetchone()
        if found_link is not None:
            cur.execute('''INSERT INTO Links (from_id, to_id) VALUES (?,?)''', 
                    (wp.pageid, found_link[0]))
        else:
            cur.execute('''INSERT INTO To_Crawl 
                    (title, added, from_id) VALUES (?,?,?)''',
                    (link, int(time()), wp.pageid))

    crawled += 1
    if crawled % COMMIT_FREQ == 0:
        conn.commit()
    

conn.commit()
conn.close()
