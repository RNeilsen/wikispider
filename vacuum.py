import sqlite3

from initialise import INDEX_FILE_PATH

conn = sqlite3.connect(INDEX_FILE_PATH, isolation_level='EXCLUSIVE')
cur = conn.cursor()

print('WARNING: Ensure no spiders/indexers are running on database!')
cont = input('Continue? (Y/n)')
if not (cont == '' or cont.lower() == 'y'):
    print('Aborting...')
    exit()

print('Vacuuming...', end='', flush=True)
cur.execute('VACUUM')
conn.commit()
conn.close()
print('complete!')
