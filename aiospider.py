import asyncio, time, sqlite3, wikipedia
from aioify import aioify
from initialise import INDEX_FILE_PATH

PAGE_TEXT_DIRECTORY = 'dbs/pagetext/'
PAGES_PER_BATCH = 50
BATCHES_PER_COMMIT = 4

wikipedia.set_rate_limiting(True)

pages = ["Mathematics", "Emmy Noether", "Group Theory", "Carl Gauss", "Richard Feynman", 
        "Quotient group", "Carl Sagan", "Abstract algebra", "Ring theory", "Particle physics",
        "Python (programming language)", "Existential quantifier", "Tree", "Tree (graph theory)",
        "Toblerone", "Balsamic vinegar", "Beer"]
        
aiowp = aioify(obj=wikipedia.page)


def check_out_rows(cur, max_to_fetch=0):
    if max_to_fetch == 0:
        max_to_fetch = PAGES_PER_BATCH * BATCHES_PER_COMMIT
    else:
        max = min(max_to_fetch, PAGES_PER_BATCH * BATCHES_PER_COMMIT)
        if max < 1: max = 1

    cur.execute(''' SELECT DISTINCT title, pageid FROM Crawl_Queue
                    WHERE status < 30
                    ORDER BY status ASC, added ASC
                    LIMIT ? ''', (max,))
    rows = cur.fetchall()
    for (title, pageid) in rows:
        if pageid is not None:
            cur.execute(''' UPDATE Crawl_Queue 
                    SET status = 30 
                    WHERE pageid=? ''', (pageid,))
        elif title is not None:
            cur.execute(''' UPDATE Crawl_Queue 
                    SET status = 30 
                    WHERE title=? ''', (title,))
        else:
            raise Exception("Both pageid and title are None")
    cur.connection.commit()
    
    if len(rows) == 0:
        print('NOTE: No records found with status < 30 in Crawl_Queue')

    print('check_out_rows() checked out', len(rows), 'distinct titles.')
    return rows


async def get_page(cur, title=None, pageid=None):
    '''Download page contents, produce a set of queries to update the db'''
    if title is None and pageid is None:
        raise Exception("get_page called with no title or pageid")
    
    # Attempt to open the page
    start_time = time.perf_counter()
    if pageid is not None:
        try:
            wp = await aiowp(pageid=pageid, auto_suggest=False, preload=True)
        except Exception as e:
            print(f'ERROR: Searching pageid {pageid} with preload produced', 
                    'the following exception:', flush=True)
            print(e)
            print('Attempting without preload...', flush=True)
            try:
                wp = await aiowp(pageid=pageid, auto_suggest=False, preload=False)
            except Exception as e2:
                print(f'ERROR: Searching pageid {pageid} again without preload',
                        'failed with the following exception:', flush=True)
                print(e2)
                print('Will flag record in Crawl_Queue with status code 90.')
                return ( [('''UPDATE Crawl_Queue SET status=90
                        WHERE pageid=?''', (pageid,))], [] )
    else:
        try:
            wp = await aiowp(title=title, auto_suggest=False, preload=True)
        except wikipedia.PageError:
            print(f'ERROR: Searched title {repr(title)}, page not found.',
                    'Will flag record in Crawl_Queue with status code 70.')
            return ( [('''UPDATE Crawl_Queue SET status=70
                    WHERE title=?''', (title,))], [] )
        except Exception as e:
            if repr(e) == 'extlinks':
                print(f"ERROR: Searching title {repr(title)} with preload produced" +
                        "'extlinks' exception, retrying without preload")
            else:
                print(f'ERROR: Searching title {repr(title)} with preload produced',
                        'the following exception:', flush=True)
                print(e)
                print('Attempting without preload...', flush=True)
            try:
                wp = await aiowp(title=title, auto_suggest=False, preload=False)
            except Exception as e2:
                print(f'ERROR: Searched title {repr(title)} again without preload',
                        'failed with the following exception:', flush=True)
                print(e2)
                print('Will flag record in Crawl_Queue with status code 90.')
                return ( [('''UPDATE Crawl_Queue SET status=90
                        WHERE title=?''', (title,))], [] )
    print(wp, f"found in {time.perf_counter() - start_time:0.1f}s", flush=True)
    status_code = 40

    queries = []
    raw_texts = []
    
    # Resolve all links in Crawl_Queue with this page as their target
    cur.execute('''SELECT from_id FROM Crawl_Queue WHERE
            title=? OR title=?''', (title, wp.title))
    resolved_link_from_ids = [item[0] for item in cur.fetchall()]

    for resolved_from_id in resolved_link_from_ids:
        queries.append( ('''INSERT OR IGNORE INTO Links (to_id, from_id)
                VALUES (?,?)''', (int(wp.pageid), resolved_from_id)) )

    queries.append( ('''DELETE FROM Crawl_Queue WHERE
            (title=? OR title=?)''', (title, wp.title)) )

    # Resolve any of this page's links we can and add the rest to Crawl_Queue
    try:
        links = wp.links.copy()
    except Exception as e:
        print(f'ERROR: Retrieving links from', wp, 
                'produced the following exception:')
        print(e)
        print('Will enter into Pages with status code 60')
        links_error = True
    else:
        links_error = False
    
    if len(links) > 0:
        resolved_links = {}
        # if len(links) > 1000:
        #     print(f'NOTE: Page {wp.pageid}: {repr(wp.title)} has {len(links)} links.')
        while len(links) > 0:
            links_to_pull = links[:min(len(links), 990)]
            del links[:len(links_to_pull)]
            cur.execute('''SELECT title, pageid FROM Pages WHERE ''' + 
                    ' OR '.join(['title=?' for _ in links_to_pull]),
                    tuple(links_to_pull))
            resolved_links.update({title: pageid for title in cur.fetchall()})
        unresolved_links = []
        for title in links:
            if title in resolved_links:
                queries.append( ('''INSERT OR IGNORE INTO Links (from_id, to_id)
                        VALUES (?,?)''', (int(wp.pageid), resolved_links[title])) )
            else:
                queries.append( ('''INSERT OR IGNORE INTO Crawl_Queue 
                        (title, added, from_id) VALUES (?,?,?)''', 
                        (title, int(time.time()), int(wp.pageid))) )

    # Enter this page into the db and save raw text
    try:
        raw_text = wp.content
    except Exception as e:
        print(f'ERROR: Retrieving content from', wp, 
                'produced the following exception:')
        print(e)
        print('Will enter into Pages with status code 60')
        queries.append( ('''INSERT OR REPLACE INTO Pages
                (pageid, title, status, crawled) VALUES (?,?,?,?)''',
                (wp.pageid, wp.title, 60, int(time.time()))) )
    else:
        queries.append( ('''INSERT OR REPLACE INTO Pages
                (pageid, title, status, crawled) VALUES (?,?,?,?)''',
                (wp.pageid, wp.title, (60 if links_error else 40), 
                int(time.time()))) )
        raw_texts.append((wp.pageid, raw_text))

    return (queries, raw_texts)
    

async def main():
    try:
        num_to_crawl = int(input(f"Crawl how many pages? ({PAGES_PER_BATCH}) "))
    except ValueError:
        num_to_crawl = PAGES_PER_BATCH
    
    conn = sqlite3.connect(INDEX_FILE_PATH)
    cur = conn.cursor()

    num_crawled = 0
    batches_complete = 0
    rows = []
    query_queue = []
    raw_text_queue = []
    while num_crawled < num_to_crawl:
        # run one batch
        if len(rows) == 0:
            rows = check_out_rows(cur, num_to_crawl - num_crawled)
        
        coro_queue = []
        num_for_this_batch = min(len(rows), PAGES_PER_BATCH)

        # Crawl PAGES_PER_BATCH pages simultaneously, building query queue
        for _ in range(num_for_this_batch):
            (title, pageid) = rows.pop()
            if pageid is not None:
                coro_queue.append(get_page(cur, pageid=pageid))
            else:
                coro_queue.append(get_page(cur, title=title))
        
        results = await asyncio.gather(*coro_queue)
        for (queries, raw_texts) in results:
            query_queue += queries
            raw_text_queue += raw_texts
        num_crawled += len(results)

        batches_complete += 1
        if batches_complete % BATCHES_PER_COMMIT == 0:
            print(f'{num_crawled} / {num_to_crawl} pages crawled.', 
                    'Executing query queue...', end='', flush=True)
            for (q, v) in query_queue:
                # print ('Executing:', q, v, flush=True)
                cur.execute(q, v)
            query_queue = []
            print('done. Writing .wsr files...', end='', flush=True)
            for (pageid, raw_text) in raw_text_queue:
                with open(PAGE_TEXT_DIRECTORY + str(pageid) + '.wsr', 'w') as f:
                    f.write(raw_text)
            raw_text_queue = []
            print('done. Committing...', end='', flush=True)
            conn.commit()
            print('done.', flush=True)
            
    print(f'{num_crawled} / {num_to_crawl} pages crawled.', 
            'Executing query queue...', end='', flush=True)
    for (q, v) in query_queue:
        # print ('Executing:', q, v, flush=True)
        cur.execute(q, v)
    query_queue = []
    print('done. Writing .wsr files...', end='', flush=True)
    for (pageid, raw_text) in raw_text_queue:
        with open(PAGE_TEXT_DIRECTORY + str(pageid) + '.wsr', 'w') as f:
            f.write(raw_text)
    raw_text_queue = []
    print('done. Committing...', end='', flush=True)
    conn.commit()
    print('done.', flush=True)
    
    conn.commit()
    conn.close()


if __name__ == '__main__':
    start = time.perf_counter()
    async_res = asyncio.run(main())
    print(f'Completed in {time.perf_counter() - start:0.2f}s')