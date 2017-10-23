#!/usr/bin/env python3

""" query ampache mysql and merge into rhythmbox db

  merge data from ampache with rhythmbox
  --------------------------------------

  This script will examine a dump file from lastscrape
  then query your ampache database

  if it matches the artist, album and song
  it will update your databse to reflect each play

"""


# import codecs
import csv
import os
import shutil
# import sys
import mysql.connector
import urllib.parse
import xml.etree.ElementTree as etree


# Connection and queries
cnx = None
playcursor = None
ratingcursor = None

# Default file names
settings = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.csv')

# Default paths for rhythmbox & the user
HOMEFOLDER = os.getenv('HOME')
PATH = '/.local/share/rhythmbox/'
DB = (HOMEFOLDER + PATH + 'rhythmdb.xml')
DBBACKUP = (HOMEFOLDER + PATH + 'rhythmdb-backup-merge.xml')

FIND = None
REPLACE = None

# get settings for database
if os.path.isfile(settings):
    print('found settings file')
    with open(settings, 'r') as csvfile:
        openfile = csv.reader(csvfile)
        for row in openfile:
            try:
                test = row[0]
            except IndexError:
                test = None
            if test:
                if row[0] == 'dbuser':
                    dbuser = row[1]
                elif row[0] == 'dbpass':
                    dbpass = row[1]
                elif row[0] == 'dbhost':
                    dbhost = row[1]
                elif row[0] == 'dbname':
                    dbname = row[1]
                elif row[0] == 'myid':
                    myid = row[1]
                elif row[0] == 'find':
                    FIND = row[1]
                elif row[0] == 'replace':
                    REPLACE = row[1]
    csvfile.close()
else:
    # Database variables
    dbuser = 'username'
    dbpass = 'password'
    dbhost = '127.0.0.1'
    dbname = 'database'

    # Ampache variables
    myid = '2'


try:
    cnx = mysql.connector.connect(user=dbuser, password=dbpass,
                                  host=dbhost, database=dbname,
                                  connection_timeout=5)
except mysql.connector.errors.InterfaceError:
    try:
        cnx = mysql.connector.connection.MySQLConnection(user=dbuser,
                                                         password=dbpass,
                                                         host=dbhost,
                                                         database=dbname,
                                                         connection_timeout=5)
    except mysql.connector.errors.InterfaceError:
        pass
#
# Try to get through with ssh fowarding
#
# eg. ssh -L 3306:externalhost:3306 externalhost
#
if not cnx:
    try:
        cnx = mysql.connector.connect(user=dbuser, password=dbpass,
                                      host='localhost', database=dbname, connection_timeout=5)
    except mysql.connector.errors.InterfaceError:
        pass
if cnx:
    # only start if the database has been backed up.
    try:
        print('creating rhythmdb backup\n')
        shutil.copy(DB, DBBACKUP)
        DBBACKUP = True
    except FileNotFoundError:
        DBBACKUP = False
    except PermissionError:
        DBBACKUP = False

if cnx and DBBACKUP:
    print('Connection Established\n')
    playcursor = cnx.cursor()
    executionlist = []
    # total count of plays
    playquery = ('SELECT DISTINCT song.title, artist.name, album.name, ' +
                 'CASE WHEN song.mbid IS NULL THEN \'\' ELSE song.mbid END as smbid, ' +
                 'CASE WHEN artist.mbid IS NULL THEN \'\' ELSE artist.mbid END as ambid, ' +
                 'CASE WHEN album.mbid IS NULL THEN \'\' ELSE album.mbid END as almbid, ' +
                 'COUNT(object_count.object_id), ' +
                 'song.file ' +
                 'FROM object_count ' +
                 'INNER JOIN song on song.id = object_count.object_id AND object_count.object_type = \'song\' ' +
                 'LEFT JOIN artist on artist.id = song.artist ' +
                 'LEFT JOIN album on album.id = song.album ' +
                 'WHERE object_count.object_type = \'song\' ' +
                 'GROUP BY song.title, artist.name, album.name, smbid, ambid, almbid;')
    try:
        playcursor.execute(playquery)
    except mysql.connector.errors.ProgrammingError:
        print('ERROR WITH QUERY:\n' + playquery)





# only process id db found and backup created.
if os.path.isfile(DB) and DBBACKUP:
    print('Connection Established\n')
    # search for plays by artist, track AND album
    # open the database
    print('Opening rhythmdb for play counts...\n')
    root = etree.parse(os.path.expanduser(DB)).getroot()
    items = [s for s in root.getiterator("entry")
             if s.attrib.get('type') == 'song']
    if items and cnx:
        RBCACHE = []
        RBFILECACHE = []
        print('Building song data for play counts...\n')
        for entries in items:
            if entries.attrib.get('type') == 'song':
                data = {}
                filedata = {}
                for info in entries:
                    if info.tag in ('title', 'artist', 'album', 'mb-trackid', 'mb-artistid', 'mb-albumid'):
                        data[info.tag] = urllib.parse.unquote(info.text.lower())
                    if info.tag in 'location':
                        filedata[info.tag] = urllib.parse.unquote(info.text).lower().replace('file://', '')
            try:
                RBCACHE.append('%(title)s\t%(artist)s\t%(album)s\t%(mb-trackid)s' +
                               '\t%(mb-artistid)s\t%(mb-albumid)s' % data)
            except KeyError:
                RBCACHE.append('%(title)s\t%(artist)s\t%(album)s\t\t\t' % data)
            RBFILECACHE.append('%(location)s' % filedata)
        print('Processing mysql play counts\n')
        if playcursor:
            changemade = False
            for row in playcursor:
                tmprow = []
                tmpsong = None
                tmpartist = None
                tmpalbum = None
                tmpentry = None
                mergeplays = False
                foundartist = None
                foundalbum = None
                foundsong = None
                idx = None
                tmpcheck = None
                tmpfilecheck = None
                # Require a minimum of Date, Title, Artist, Album
                try:
                    test = [row[0], row[1], row[2], row[3]]
                except IndexError:
                    test = None
                # Using the last.fm data check for the same song in rhythmbox
                if test:
                    if not mergeplays:
                        # Check for a match using the id3 tags
                        tmpcheck = (str(row[0].lower()) + '\t' + str(row[1].lower()) + '\t' +
                                    str(row[2].lower()) + '\t' + str(row[3]).replace('None', '') + '\t' +
                                    str(row[4]).replace('None', '') + '\t' + str(row[5]).replace('None', ''))
                        if tmpcheck in RBCACHE:
                            idx = RBCACHE.index(tmpcheck)
                    if not idx:
                        # When you can't match tags, check filename
                        if FIND and REPLACE:
                            tmpfilecheck = str(row[7].lower()).replace(FIND, REPLACE)
                        else:
                            tmpfilecheck = str(row[7].lower())
                        if tmpfilecheck in RBFILECACHE:
                            idx = RBFILECACHE.index(tmpfilecheck)
                # if the index is found, update the playcount
                if idx:
                    entry = items[idx]
                    tmpplay = '0'
                    for info in entry:
                        if info.tag == 'play-count':
                            tmpplay = str(info.text)
                            if str(info.text) == str(row[6]):
                                mergeplays = True
                            elif not str(info.text) == str(row[6]):
                                changemade = True
                                print('Updating playcount for', row[0], 'from ' + tmpplay + ' to', row[6])
                                info.text = str(row[6])
                                mergeplays = True
                    if not mergeplays:
                        changemade = True
                        print('Inserting playcount for', row[0], 'as', row[6])
                        insertplaycount = etree.SubElement(entry, 'play-count')
                        insertplaycount.text = str(row[6])
                        mergeplays = True
                # if not mergeplays:
                #    print('entry not found')
                #    #print(row)
                #    print(tmpcheck)
            if changemade:
                print('Plays from mysql have been inserted into the database.\n')
                # Save changes
                print('saving changes')
                output = etree.ElementTree(root)
                output.write(os.path.expanduser(DB), encoding="utf-8")
            else:
                print('No play counts changed')
    else:
        print('no play data found\n')
else:
    # there was a problem with the command
    print('FILE NOT FOUND.\nUnable to process\n')


if cnx:
    print('Connection Established\n')
    ratingcursor = cnx.cursor(buffered=True)
    executionlist = []
    # ampache ratings for all songs
    ratingquery = ('SELECT DISTINCT song.title, artist.name, album.name, ' +
                   'CASE WHEN song.mbid IS NULL THEN \'\' ELSE song.mbid END as smbid, ' +
                   'CASE WHEN artist.mbid IS NULL THEN \'\' ELSE artist.mbid END as ambid, ' +
                   'CASE WHEN album.mbid IS NULL THEN \'\' ELSE album.mbid END as almbid, ' +
                   'rating.rating, ' +
                   'song.file ' +
                   'FROM rating ' +
                   'INNER JOIN song on song.id = rating.object_id AND rating.object_type = \'song\' ' +
                   'LEFT JOIN artist on artist.id = song.artist ' +
                   'LEFT JOIN album on album.id = song.album ' +
                   'WHERE rating.object_type = \'song\' AND ' +
                   'rating.user = ' + str(myid))
    try:
        ratingcursor.execute(ratingquery)
    except mysql.connector.errors.ProgrammingError as e:
        print('ERROR WITH QUERY:\n' + ratingquery)
        print(e)

    if ratingcursor and DBBACKUP:
        print('Opening rhythmdb...\n')
        root = etree.parse(os.path.expanduser(DB)).getroot()
        items = [s for s in root.getiterator("entry")
                 if s.attrib.get('type') == 'song']
        RBCACHE = []
        RBFILECACHE = []
        print('Building song data for ratings...\n')
        for entries in items:
            if entries.attrib.get('type') == 'song':
                data = {}
                filedata = {}
                for info in entries:
                    if info.tag in ('title', 'artist', 'album', 'mb-trackid', 'mb-artistid', 'mb-albumid'):
                        data[info.tag] = urllib.parse.unquote(info.text.lower())
                    if info.tag in 'location':
                        filedata[info.tag] = urllib.parse.unquote(info.text).lower().replace('file://', '')
            try:
                RBCACHE.append('%(title)s\t%(artist)s\t%(album)s\t%(mb-trackid)s' +
                               '\t%(mb-artistid)s\t%(mb-albumid)s' % data)
            except KeyError:
                RBCACHE.append('%(title)s\t%(artist)s\t%(album)s\t\t\t' % data)
            RBFILECACHE.append('%(location)s' % filedata)
        print('Processing mysql track ratings\n')
        if ratingcursor:
            changemade = False
            for row in ratingcursor:
                tmprow = []
                tmpsong = None
                tmpartist = None
                tmpalbum = None
                tmpentry = None
                mergeplays = False
                foundartist = None
                foundalbum = None
                foundsong = None
                idx = None
                tmpcheck = None
                tmpfilecheck = None
                # Require a minimum of Date, Title, Artist, Album
                try:
                    test = [row[0], row[1], row[2], row[3]]
                except IndexError:
                    test = None
                # Using the last.fm data check for the same song in rhythmbox
                if test:
                    if not mergeplays:
                        # Check for a match using the id3 tags
                        tmpcheck = (str(row[0].lower()) + '\t' + str(row[1].lower()) + '\t' +
                                    str(row[2].lower()) + '\t' + str(row[3]).replace('None', '') + '\t' +
                                    str(row[4]).replace('None', '') + '\t' + str(row[5]).replace('None', ''))
                        if tmpcheck in RBCACHE:
                            idx = RBCACHE.index(tmpcheck)
                    if not idx:
                        # When you can't match tags, check filename
                        if FIND and REPLACE:
                            tmpfilecheck = str(row[7].lower()).replace(FIND, REPLACE)
                        else:
                            tmpfilecheck = str(row[7].lower())
                        if tmpfilecheck in RBFILECACHE:
                            idx = RBFILECACHE.index(tmpfilecheck)
                # if the index is found, update the playcount
                if idx:
                    # print(idx)
                    entry = items[idx]
                    for info in entry:
                        if info.tag == 'rating':
                            if str(info.text) == str(row[6]):
                                mergeplays = True
                            elif not str(info.text) == str(row[6]):
                                changemade = True
                                info.text = str(row[6])
                                mergeplays = True
                    if not mergeplays:
                        changemade = True
                        print('Inserting rating for', row[0], 'as', row[6])
                        insertplaycount = etree.SubElement(entry, 'rating')
                        insertplaycount.text = str(row[6])
                        mergeplays = True
                # if not mergeplays:
                #    print('entry not found')
                #    #print(row)
                #    print(tmpcheck)
            if changemade:
                print('Ratings from mysql have been rated in the database.\n')
                # Save changes
                print('saving changes')
                output = etree.ElementTree(root)
                output.write(os.path.expanduser(DB), encoding="utf-8")
            else:
                print('No Ratings were updated.')
    else:
        print('no rating data found\n')
else:
    # there was a problem with the command
    print('FILE NOT FOUND.\nUnable to process\n')

print('Done\n')
