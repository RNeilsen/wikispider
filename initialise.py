'''Completely wipes and resets the databases'''

import os, sqlite3

dbs = { 'dump.sqlite': '''
            CREATE TABLE Pages
            (   url         TEXT NOT NULL PRIMARY KEY UNIQUE,
                raw_html    TEXT,
                crawled     INTEGER )''',
        
        'to_crawl.sqlite': '''
            CREATE TABLE Urls
            (   url         TEXT NOT NULL PRIMARY KEY UNIQUE,
                processed   INTEGER NOT NULL )''',
        
        'index.sqlite': '''
            CREATE TABLE Pages
            (   url         TEXT NOT NULL PRIMARY KEY UNIQUE,
                comp_text   TEXT,
                crawled     INTEGER );
            CREATE TABLE Links
            (   from_url    TEXT,
                to_url      TEXT,
                UNIQUE (from_url, to_url) );
            CREATE TABLE Words
            (   id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                word        TEXT );
            CREATE TABLE Mentions
            (   word_id     INTEGER NOT NULL,
                url         TEXT NOT NULL,
                position    INTEGER NOT NULL )'''}

for f in dbs.keys():
    if os.path.isfile(f):
        resp = input('Delete ' + f + '? (y/N) ')
        if resp.lower() != 'y':
            continue
        os.remove(f)
    else:
        print(f, 'not found, initialising')

    conn = sqlite3.connect(f)
    cur = conn.cursor()

    cur.executescript(dbs[f])

    conn.commit()
    conn.close()
