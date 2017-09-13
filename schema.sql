CREATE TABLE cache (
   id INTEGER PRIMARY KEY,
   artist TEXT NOT NULL,
   release_title TEXT NOT NULL,
   image BLOB,
   unique (artist, release_title)
);

