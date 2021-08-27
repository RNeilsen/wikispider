'''Downloads pages and enters new links to be crawled'''

import sqlite3, wikipedia
from time import time

MAX_CONSEC_FAILS = 5
COMMIT_FREQ = 1
MAX_ROWS_AT_A_TIME = 10
DO_PRELOAD = False
RECRAWL_TIME = 86400        # num seconds before recrawling a previously-crawled page
FAILURE_PENALTY = 86400     # num seconds to add to crawl time for a failed pageload
wikipedia.set_rate_limiting(True)

def get_more_rows(cur, max_to_fetch):
    max_to_fetch = min(MAX_ROWS_AT_A_TIME, max_to_fetch)
    if max_to_fetch < 1: max_to_fetch = 1
    
    # 
    # [ ( table, ( [select_fields], order_field, [(where_req, where_val)] ) ]
    search_order = [ 
            # ( 'Pages',      ( ['pageid', 'title'], 'crawled', [('raw_text', None)] ) ),
            ( 'Open_Links', ( ['NULL', 'title'], 'added', [] ) ),
            ( 'Pages',      ( ['pageid', 'title'], 'crawled', [] ) )
    ]

    rows = []
    for (table, (retrieve_fields, order_field, where_clauses)) in search_order:
        stmt = f"SELECT {', '.join(retrieve_fields)} FROM {table} "
        
        where_values = tuple()
        if where_clauses is not None and len(where_clauses) > 0:
            stmt += 'WHERE 1 '
            for (where_col, where_val) in where_clauses:
                if where_val is None:
                    stmt += 'AND ' + where_col + ' IS NULL '
                else:
                    stmt += 'AND ' + where_col + '=? '
                    where_values += (where_val,)
        
        if order_field is not None:
            stmt += f'ORDER BY {order_field} '

        stmt += f'LIMIT {max_to_fetch - len(rows)}'
        print("Executing:", stmt, where_values)
        cur.execute(stmt, where_values)
        rows += cur.fetchall()

        if len(rows) >= max_to_fetch:
            return rows

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
(crawled, fails) = (0, 0)

while crawled < num_to_crawl:
    if fails >= MAX_CONSEC_FAILS:
        print(f'{fails} failures in a row, terminating...')
        break

    if crawled % COMMIT_FREQ == 0:
        conn.commit()
    
    if len(rows) == 0:
        conn.commit()
        rows = get_more_rows(cur, num_to_crawl - crawled)
    
    (pageid, title) = rows.pop()
    
    # handling disambig pages is not yet supported
    if title.endswith('(disambiguation)'):
        print(f"Warning: {title} in links list, replacing in Open_Links")
        cur.execute('''UPDATE Open_Links SET added=? WHERE title = ?''' , 
                (crawl_time + FAILURE_PENALTY, title) )
        continue

    # Fetch page
    print("Attempting to open", (pageid, title), "... ", end='', flush=True)
    crawl_time = int(time())
    if pageid is not None:
        wp = wikipedia.page(pageid=pageid, preload=DO_PRELOAD)
    else:
        try:
            wp = wikipedia.page(title, preload=DO_PRELOAD, auto_suggest=False)
        except wikipedia.exceptions.PageError:
            print('Could not find title, replacing in Open_Links')
            fails += 1
            cur.execute('''UPDATE Open_Links SET added=? WHERE title = ?''' , 
                    (crawl_time + FAILURE_PENALTY, title) )
            continue
    print('success!', wp, flush=True)
    pageid = wp.pageid
    
    crawled += 1
    fails = 0

    # Resolve all links to this title in Open_Links
    cur.execute('''SELECT from_id FROM Open_Links WHERE title=? OR title=?''',
            (title, wp.title))
    from_ids = cur.fetchall()
    cur.executemany(f'''DELETE FROM Open_Links WHERE 
            (title=? OR title=?) AND (from_id IS NULL OR from_id=?)''', 
            [(title, wp.title, from_id[0]) for from_id in from_ids] )
    cur.executemany(f'''INSERT OR IGNORE INTO Links (from_id, to_id) VALUES (?,?)''',
            [(from_id[0], pageid) for from_id in from_ids if from_id is not None] )

    # Check if we've already crawled this page recently, skip if so
    cur.execute('''SELECT crawled FROM Pages WHERE pageid=?''', (wp.pageid,))
    found_record = cur.fetchone()
    if found_record is not None:
        if found_record[0] is not None and found_record[0] >= crawl_time - RECRAWL_TIME:
            continue

    # Enter page into Pages
    cur.execute('''INSERT OR REPLACE INTO Pages 
            (pageid, title, raw_text, crawled) VALUES (?, ?, ?, ?)''',
            (pageid, wp.title, wp.content, crawl_time) )

    # Add all of this article's links into Links (if already crawled) or Open_Links (if not)
    links = wp.links
    for link in links:
        cur.execute('''SELECT pageid FROM Pages WHERE title=?''', (link,))
        found_link = cur.fetchone()
        if found_link is not None:
            cur.execute('''INSERT OR IGNORE INTO Links (from_id, to_id) VALUES (?,?)''', 
                    (pageid, found_link[0]))
        else:
            cur.execute('''INSERT OR IGNORE INTO Open_Links 
                    (title, added, from_id) VALUES (?,?,?)''',
                    (link, crawl_time, pageid))

    
conn.commit()
conn.close()
