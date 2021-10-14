"""Extract all raw_text and zip_text from the Pages table of the index
and save them into separate text files to reduce db overhead"""

from initialise import INDEX_FILE_PATH
import os, sqlite3

PAGE_TEXT_DIRECTORY = 'dbs/pagetext/'
PAGES_PER_BATCH = 100

def fetch_rows(cur, text_field):
    cur.execute(f''' SELECT pageid, {text_field} FROM Pages
                    WHERE {text_field} IS NOT NULL LIMIT ?''',
                (PAGES_PER_BATCH,))
    results = cur.fetchall()
    if len(results) == 0:
        print(f"NOTE: No more rows with non-null {text_field} remaining.")
        return []
    else:
        print(f"Fetched {len(results)} rows with non-null {text_field}")
        return results

# raw_text
conn = sqlite3.connect(INDEX_FILE_PATH)
cur = conn.cursor()

rows = [1]
while rows != []:
    rows = fetch_rows(cur, 'raw_text')
    queries = []
    for (pageid, raw_text) in rows:
        with open(f'{PAGE_TEXT_DIRECTORY + str(pageid)}.wsr', 'w') as f:
            f.write(raw_text)
            cur.execute('''UPDATE Pages SET raw_text=NULL
                    WHERE pageid=?''', (pageid,))
    print('Committing...', end='', flush=True) 
    conn.commit()
    print('Done.', flush=True)

rows = [1]
while rows != []:
    rows = fetch_rows(cur, 'zip_text')
    queries = []
    for (pageid, zip_text) in rows:
        with open(f'{PAGE_TEXT_DIRECTORY + str(pageid)}.wsb', 'wb') as f:
            f.write(zip_text)
            cur.execute('''UPDATE Pages SET zip_text=NULL
                    WHERE pageid=?''', (pageid,))
    print('Committing...', end='', flush=True) 
    conn.commit()
    print('Done.', flush=True)
