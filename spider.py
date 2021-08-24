'''Downloads pages and enters new links to be crawled'''

import sqlite3, wikipedia
from time import time

MAX_CONSEC_FAILS = 5
COMMIT_FREQ = 1
wikipedia.set_rate_limiting(True)

def get_more_rows(cur, max_to_fetch):
    cur.execute(f'SELECT NULL, title FROM Open_Links ORDER BY added LIMIT {max_to_fetch}')
    rows = []
    more_rows = cur.fetchall()
    if more_rows is None:
        print("Warning: no rows found in Open_Links table")
    else:
        rows += more_rows
    if len(rows) < max_to_fetch:
        cur.execute(f'SELECT page_id, title FROM Pages ORDER BY crawled LIMIT {max_to_fetch - len(rows)}')
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
        conn.commit()
        rows = get_more_rows(cur, num_to_crawl - crawled)
    
    (page_id, title) = rows.pop()
    
    # Fetch page
    print("Attempting to open", (page_id, title), "... ", end='', flush=True)
    crawl_time = int(time())
    if page_id is not None:
        wp = wikipedia.page(pageid=page_id, preload=True)
    else:
        try:
            wp = wikipedia.page(title, preload=True, auto_suggest=False)
        except wikipedia.exceptions.PageError:
            print('Could not find title, replacing in Open_Links')
            fails += 1
            cur.execute('''UPDATE Open_Links SET added=? WHERE title = ?''' , 
                    (crawl_time + 120, title) )
            continue
    print('success!', wp, flush=True)

    # Enter page into Pages
    cur.execute('''INSERT OR REPLACE INTO Pages 
            (page_id, title, raw_text, crawled) VALUES (?, ?, ?, ?)''',
            (wp.pageid, wp.title, wp.content, crawl_time) )
    
    # Resolve all links to this title in Open_Links
    cur.execute('''SELECT from_id FROM Open_Links WHERE title=? OR title=?''',
            (title, wp.title))
    from_ids = cur.fetchall()
    cur.executemany(f'''DELETE FROM Open_Links WHERE 
            (title=? OR title=?) AND (from_id IS NULL OR from_id=?)''', 
            [(title, wp.title, from_id[0]) for from_id in from_ids] )
    cur.executemany(f'''INSERT OR IGNORE INTO Links (from_id, to_id) VALUES (?,?)''',
            [(from_id[0], wp.pageid) for from_id in from_ids] )
    
    # Add all of this article's links into Links (if already crawled) or Open_Links (if not)
    links = wp.links
    for link in links:
        cur.execute('''SELECT page_id FROM Pages WHERE title=?''', (link,))
        found_link = cur.fetchone()
        if found_link is not None:
            cur.execute('''INSERT OR IGNORE INTO Links (from_id, to_id) VALUES (?,?)''', 
                    (wp.pageid, found_link[0]))
        else:
            cur.execute('''INSERT OR IGNORE INTO Open_Links 
                    (title, added, from_id) VALUES (?,?,?)''',
                    (link, crawl_time, wp.pageid))

    crawled += 1
    if crawled % COMMIT_FREQ == 0:
        conn.commit()
    

conn.commit()
conn.close()
