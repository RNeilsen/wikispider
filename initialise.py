'''Completely wipes and resets the database, and inserts a few page
titles to be crawled as a starting seed'''

INDEX_FILE_PATH = 'dbs/wsindex.sqlite'
import os, sqlite3

inits = { INDEX_FILE_PATH : '''
            DROP TABLE IF EXISTS Crawl_Queue;
            DROP TABLE IF EXISTS Pages;
            DROP TABLE IF EXISTS Links;
            DROP TABLE IF EXISTS Words;
            DROP TABLE IF EXISTS Mentions;
            
            CREATE TABLE Crawl_Queue
            (   title       TEXT NOT NULL,
                pageid      INTEGER,
                added       INTEGER,
                from_id     INTEGER,
                status      INTEGER DEFAULT 10,
                PRIMARY KEY (title, from_id) );
            CREATE TABLE Pages
            (   pageid      INTEGER NOT NULL PRIMARY KEY UNIQUE,
                title       TEXT NOT NULL UNIQUE,
                raw_text    TEXT,
                zip_text    BLOB,
                status      INTEGER,
                crawled     INTEGER );
            CREATE TABLE Links
            (   from_id     INTEGER,
                to_id       INTEGER,
                PRIMARY KEY (from_id, to_id) );
            CREATE TABLE Words
            (   id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                word        TEXT UNIQUE );
            CREATE TABLE Mentions
            (   word_id     INTEGER NOT NULL,
                pageid      INTEGER NOT NULL,
                PRIMARY KEY (word_id, pageid) );
            
            PRAGMA journal_mode=WAL;

            INSERT INTO Crawl_Queue (title) VALUES
                ( 'Mathematics' ),
                ( 'Mathematicians' ),
                ( 'Applied_mathematics' ),
                ( 'Statistics' ),
                ( 'David_Hilbert' ),
                ( 'Richard_Feynman' ),
                ( 'dablenuidaho' );
                VACUUM; '''}

def main():
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

if __name__ == '__main__':
    main()