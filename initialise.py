'''Completely wipes and resets the database, and inserts a few page
titles to be crawled as a starting seed'''

import os, sqlite3

inits = { 'wsindex.sqlite' : '''
            DROP TABLE IF EXISTS To_Crawl;
            DROP TABLE IF EXISTS Pages;
            DROP TABLE IF EXISTS Links;
            DROP TABLE IF EXISTS Words;
            DROP TABLE IF EXISTS Mentions;
            
            CREATE TABLE To_Crawl
            (   title       TEXT,
                added       INTEGER,
                from_id     INTEGER);
            CREATE TABLE Pages
            (   pageid      INTEGER NOT NULL PRIMARY KEY UNIQUE,
                title       TEXT NOT NULL UNIQUE,
                raw_text    TEXT,
                zip_text    TEXT,
                crawled     INTEGER );
            CREATE TABLE Links
            (   from_id     INTEGER,
                to_id       INTEGER,
                PRIMARY KEY (from_id, to_id) );
            CREATE TABLE Words
            (   id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                word        TEXT );
            CREATE TABLE Mentions
            (   word_id     INTEGER NOT NULL,
                page_id     INTEGER NOT NULL,
                position    INTEGER );
            
            INSERT INTO To_Crawl (title) VALUES
                ( 'Mathematics' ),
                ( 'Mathematicians' ),
                ( 'Applied_mathematics' ),
                ( 'Statistics' ),
                ( 'David_Hilbert' ),
                ( 'Richard_Feynman' );
                VACUUM; '''}

for f in inits.keys():
    if os.path.isfile(f):
        resp = input('Wipe ' + f + '? (y/N) ')
        if resp.lower() != 'y':
            continue
    else:
        print(f, 'not found, initialising')

    conn = sqlite3.connect(f)
    cur = conn.cursor()

    cur.executescript(inits[f])

    conn.commit()
    conn.close()
