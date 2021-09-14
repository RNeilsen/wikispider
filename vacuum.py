import sqlite3
from time import perf_counter

from initialise import INDEX_FILE_PATH

conn = sqlite3.connect(INDEX_FILE_PATH, isolation_level='EXCLUSIVE')
cur = conn.cursor()

print('WARNING: Ensure no spiders/indexers are running on database!')
cont = input('Continue? (Y/n)')
if cont.lower() not in {'y', ''}:
    print('Aborting...')
    exit()

start = perf_counter()
print('Erasing all checkouts...', end='', flush=True)
cur.execute('UPDATE Crawl_Queue SET status=10 WHERE status=30')
conn.commit()
print(f'complete in {perf_counter() - start:0.1f}s')

start = perf_counter()
print('Vacuuming...', end='', flush=True)
cur.execute('VACUUM')
conn.commit()
conn.close()
print(f'complete in {perf_counter() - start:0.1f}s')
