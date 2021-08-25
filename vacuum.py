import sqlite3

conn = sqlite3.connect('wsindex.sqlite')
cur = conn.cursor()

print('Vacuuming...', end='', flush=True)
cur.execute('VACUUM')
conn.commit()
conn.close()
print('complete!')
