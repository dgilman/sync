# sync.py

sync.py loads a flash drive with mp3s, formatting them to have consistent, sortable names and adding album artwork. It adds just enough ID3 tags to please my 2015 Honda Civic. :)

## Setup

1. Install Python 3.6 and mutagen
1. Get a discogs API user token, available for free here https://www.discogs.com/settings/developers
1. Create a file to\_sync.txt that has the directories you want copied out of the source directory. If your library is organized Artist/Album it would look like this:

```
Pink Floyd
Radiohead
```

You can put in subdirectories if you only want a few albums from an artist:

```
Pink Floyd
Radiohead/A Moon Shaped Pool
Radiohead/Amnesiac
Radiohead/The Bends
Radiohead/Hail to the Thief
Radiohead/In Rainbows
Radiohead/Kid A
Radiohead/The King of Limbs
Radiohead/OK Computer
```

would copy just the contents of the eight albums and ignore any other Radiohead album(s).

## Usage

```
export DISCOGS_USER_TOKEN=foobarbaz
python sync.py SRC_DIR DEST_DIR
```

The program will remind you to run fatsort on the disk when it's done.

## How it works

Asyncio keeps disk writes busy, useful for slow flash drives like mine. The program continually fetches mp3s from the source directory, processes their metadata and gets album artwork and writes the resulting mp3 out to a standardized hierarchy. It also comes up with placeholder track names/numbers if your tagging isn't perfect.
