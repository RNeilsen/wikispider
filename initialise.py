'''Completely wipes and resets the databases, and inserts a few pages
to be crawled as a starting seed'''

import os, sqlite3

dbs = { 'wsdump.sqlite': '''
            DROP TABLE IF EXISTS Pages;
            CREATE TABLE Pages
            (   url         TEXT NOT NULL PRIMARY KEY UNIQUE,
                raw_html    TEXT,
                crawled     INTEGER,
                cleaned     INTEGER );
            INSERT INTO Pages (url) VALUES 
                ( 'https://en.wikipedia.org/wiki/Mathematics' ),
				( 'https://en.wikipedia.org/wiki/Mathematicians' ),
				( 'https://en.wikipedia.org/wiki/Applied_mathematics' ),
				( 'https://en.wikipedia.org/wiki/Statistics' ),
				( 'https://en.wikipedia.org/wiki/David_Hilbert' ),
				( 'https://en.wikipedia.org/wiki/Richard_Feynman' );
            VACUUM; ''',
        
        'wsindex.sqlite': '''
            DROP TABLE IF EXISTS Pages;
            CREATE TABLE Pages
            (   id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                url         TEXT NOT NULL UNIQUE,
                zip_text    TEXT,
                crawled     INTEGER );
            DROP TABLE IF EXISTS Links;
            CREATE TABLE Links
            (   from_id     INTEGER,
                to_id       INTEGER,
                UNIQUE (from_id, to_id) );
            DROP TABLE IF EXISTS Words;
            CREATE TABLE Words
            (   id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                word        TEXT );
            DROP TABLE IF EXISTS Mentions;
            CREATE TABLE Mentions
            (   word_id     INTEGER NOT NULL,
                page_id     TEXT NOT NULL,
                position    INTEGER NOT NULL );
            VACUUM; '''}

for f in dbs.keys():
    if os.path.isfile(f):
        resp = input('Wipe ' + f + '? (y/N) ')
        if resp.lower() != 'y':
            continue
    else:
        print(f, 'not found, initialising')

    conn = sqlite3.connect(f)
    cur = conn.cursor()

    cur.executescript(dbs[f])

    conn.commit()
    conn.close()
