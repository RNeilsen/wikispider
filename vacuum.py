from indexer import INDEX_FILE_PATH
import sqlite3

from initialise import INDEX_FILE_PATH

conn = sqlite3.connect(INDEX_FILE_PATH)
cur = conn.cursor()

print('Vacuuming...', end='', flush=True)
cur.execute('VACUUM')
conn.commit()
conn.close()
print('complete!')
