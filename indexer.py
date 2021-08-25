import sqlite3
import zlib

conn = sqlite3.connect('wsindex.sqlite', timeout=20.0)
cur = conn.cursor()

MAX_ROWS_AT_A_TIME = 10
COMMIT_FREQ = 1

def get_more_rows(max_to_fetch):
    max_to_fetch = min(max_to_fetch, MAX_ROWS_AT_A_TIME)
    if max_to_fetch < 1: max_to_fetch = 1
    cur.execute('''SELECT page_id, title, raw_text, crawled FROM Pages 
                WHERE zip_text IS NULL 
                ORDER BY crawled LIMIT ?''', (max_to_fetch,))
    return cur.fetchall()

with open('stopwords.txt') as f:
    stop_words = {word.strip() for word in f.readlines()}

try:
    num_to_index = int(input("Index how many pages? (10) "))
except ValueError:
    num_to_index = 10

rows = get_more_rows(num_to_index)
indexed = 0
while indexed < num_to_index:
    if indexed % COMMIT_FREQ == 0:
        conn.commit()
    indexed += 1

    if len(rows) == 0:
        rows = get_more_rows(num_to_index - indexed)
    (page_id, title, raw_text, crawled) = rows.pop()
    print(f'Indexing {page_id}:', title, '...', end='', flush=True)
    
    words = set(raw_text.lower().split()).difference(stop_words)

    for word in words:
        cur.execute('''SELECT id FROM Words WHERE word=?''', (word,))
        result = cur.fetchone()
        if result is None:
            cur.execute('''INSERT OR IGNORE INTO Words (word) VALUES (?)''', (word,))
            cur.execute('''SELECT id FROM Words WHERE word=?''', (word,))
            result = cur.fetchone()
        word_id = result[0]

        cur.execute('''INSERT OR IGNORE INTO Mentions (word_id, page_id) VALUES (?,?)''',
                (word_id, page_id))
    
    cur.execute('''REPLACE INTO Pages (page_id, title, zip_text, crawled) VALUES (?,?,?,?)''', 
            (page_id, title, zlib.compress(raw_text.encode()), crawled))

    print('success!', flush=True)
    
conn.commit()
conn.close()
