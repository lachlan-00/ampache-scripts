#!/usr/bin/env python3

""" mysql dump into ampache from tsv

  merge last.fm data with ampache
  -------------------------------

  This script will examine a dump file from lastscrape
  then query your ampache database

  if it matches the artist, album and song
  it will update your database to reflect each play

"""

import csv
import os
import sys
import mysql.connector

process = None
dbuser = None
dbpass = None
dbhost = None
dbname = None
csvfile = None
settings = 'settings.csv'
dumpfile = 'dump.txt'
lovedfile = 'loved.txt'

# get dump file name from arguments
for arguments in sys.argv:
    if arguments[:3] == '/d:':
        process = 'dump'
        dumpfile = arguments[3:]
    elif os.path.isfile(dumpfile):
        process = 'dump'
    if arguments[:3] == '/l:':
        process = 'loved'
        lovedfile = arguments[3:]
    elif os.path.isfile(lovedfile):
        process = 'loved'

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
    csvfile.close()
else:
    # Database variables
    dbuser = 'username'
    dbpass = 'password'
    dbhost = '127.0.0.1'
    dbname = 'database'

    # Ampache variables
    myid = '2'

# object_count will store each play for tracks
# with separate rows for artist & album
selectquery = "SELECT * FROM `object_count` ORDER BY `id` DESC"
clearnotplayed = "UPDATE `song` SET played = 0 WHERE played = 1 and id not in (SELECT object_id from object_count)"

notfoundcount = 0
notfoundlist = []

cnx = None

try:
    cnx = mysql.connector.connect(user=dbuser, password=dbpass,
                                  host=dbhost, database=dbname)
except mysql.connector.errors.InterfaceError:
    try:
        cnx = mysql.connector.connection.MySQLConnection(user=dbuser,
                                                         password=dbpass,
                                                         host=dbhost,
                                                         database=dbname)
    except mysql.connector.errors.InterfaceError:
        pass
if cnx:
    print('Connection Established\n')
    cursor = cnx.cursor()
    executionlist = []
    if os.path.isfile(dumpfile):
        with open(dumpfile, 'r') as csvfile:
            # lastscrape is sorted recent -> oldest so reverse that
            # that way the database will have a lower ID for older tracks
            openfile = reversed(list(csv.reader(csvfile, delimiter='\t', )))
            # print tsv header to allow live updates to output file
            print('date\ttrack\tartist\talbum\ttrackmbid\tartistmbid' +
                  '\talbummbid\tampachetrack\tampacheartist\tampachealbum')
            for row in openfile:
                tmprow = []
                tmpdate = None
                tmpartist = None
                tmpalbum = None
                tmpsong = None
                founddate = False
                foundartist = None
                foundalbum = None
                foundsong = None
                rowtrack = None
                rowartist = None
                rowalbum = None
                trackmbid = None
                artistmbid = None
                albummbid = None
                try:
                    test = row[0]
                except IndexError:
                    test = None
                if test:
                    # Normalise row data
                    tmpdate = str(row[0])
                    if not row[1] == '':
                        rowtrack = row[1]
                    if not row[2] == '':
                        rowartist = row[2]
                    if not row[3] == '':
                        rowalbum = row[3]
                    if not row[4] == '':
                        trackmbid = row[4]
                    if not row[5] == '':
                        artistmbid = row[5]
                    if not row[6] == '':
                        albummbid = row[6]
                    # search ampache db for song, album and artist
                    # Look for a musicbrainz ID before the text of the tags
                    # This should hopefully be more reliable if your tags change a lot
                    #
                    # [0]=time [1]=track [2]=artist [3]=album [4]=trackMBID [5]=artistMBID [6]=albumMBID
                    #
                    if trackmbid and albummbid and artistmbid:
                        tmpquery = ("SELECT `song`.`id`, `album`.`id`, `artist`.`id` " +
                                    "FROM `song` INNER JOIN album ON `song`.`album` = `album`.`id` " +
                                    "INNER JOIN artist ON `song`.`artist` = `artist`.`id` " +
                                    "WHERE `song`.`mbid` = '" + trackmbid + "' AND " +
                                    "`album`.`mbid` = '" + albummbid + "' AND " +
                                    "`artist`.`mbid` = '" + artistmbid + "';")
                        # Check the database
                        try:
                            cursor.execute(tmpquery)
                        except mysql.connector.errors.ProgrammingError:
                            print('ERROR WITH QUERY:\n' + tmpquery)
                        for rows in cursor:
                            tmpsong = str(rows[0])
                            tmpalbum = str(rows[1])
                            tmpartist = str(rows[2])
                    if (not tmpsong or not tmpartist or not tmpalbum) and (rowtrack and rowartist and rowalbum):
                        # find missing data if the mbid didn't work
                        if not tmpsong and not tmpartist and not tmpalbum:
                            tmpquery = ("SELECT `song`.`id`, `album`.`id`, `artist`.`id` " +
                                        "FROM `song` INNER JOIN album ON `song`.`album` = `album`.`id` " +
                                        "INNER JOIN artist ON `song`.`artist` = `artist`.`id` " +
                                        "WHERE LOWER(`song`.`title`) = LOWER('" + rowtrack.replace("'", "''") +
                                        "') AND CASE WHEN album.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(album.prefix, ' ', album.name)) ELSE LOWER(album.name) " +
                                        "END = LOWER('" + rowalbum.replace("'", "''") +
                                        "') AND CASE WHEN artist.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(artist.prefix, ' ', artist.name)) ELSE LOWER(artist.name) " +
                                        "END = LOWER('" + rowartist.replace("'", "''") + "');")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                if not tmpsong:
                                    tmpsong = str(rows[0])
                                if not tmpalbum:
                                    tmpalbum = str(rows[1])
                                if not tmpartist:
                                    tmpartist = str(rows[2])
                        # try pairs
                        if (not tmpsong or not tmpartist) and (trackmbid and artistmbid):
                            tmpquery = ("SELECT `song`.`id`, `artist`.`id` " +
                                        "FROM `song` INNER JOIN artist ON `song`.`artist` = `artist`.`id` " +
                                        "WHERE `song`.`mbid` = '" + trackmbid + "' AND " +
                                        "`artist`.`mbid` = '" + artistmbid + "';")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                if not tmpsong:
                                    tmpsong = str(rows[0])
                                if not tmpartist:
                                    tmpartist = str(rows[1])
                        if (not tmpsong or not tmpartist) and (rowtrack and rowartist):
                            tmpquery = ("SELECT `song`.`id`, `artist`.`id` " +
                                        "FROM `song` INNER JOIN artist ON `song`.`artist` = `artist`.`id` " +
                                        "WHERE LOWER(`song`.`title`) = LOWER('" + rowtrack.replace("'", "''") +
                                        "') AND CASE WHEN artist.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(artist.prefix, ' ', artist.name)) ELSE LOWER(artist.name) " +
                                        "END = LOWER('" + rowartist.replace("'", "''") + "');")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                if not tmpsong:
                                    tmpsong = str(rows[0])
                                if not tmpartist:
                                    tmpartist = str(rows[1])
                        if (not tmpalbum or not tmpartist) and (albummbid and artistmbid):
                            tmpquery = ("SELECT `album`.`id`, `artist`.`id` " +
                                        "FROM `song` INNER JOIN artist ON `song`.`artist` = `artist`.`id` " +
                                        "INNER JOIN album ON `song`.`album` = `album`.`id` " +
                                        "WHERE `album`.`mbid` = '" + albummbid + "' AND " +
                                        "`artist`.`mbid` = '" + artistmbid + "';")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                if not tmpalbum:
                                    tmpalbum = str(rows[0])
                                if not tmpartist:
                                    tmpartist = str(rows[1])
                        if (not tmpalbum or not tmpartist) and (rowalbum and rowartist):
                            tmpquery = ("SELECT `song`.`id`, `artist`.`id` " +
                                        "FROM `song` INNER JOIN artist ON `song`.`artist` = `artist`.`id` " +
                                        "INNER JOIN album ON `song`.`album` = `album`.`id` " +
                                        "WHERE CASE WHEN album.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(album.prefix, ' ', album.name)) ELSE LOWER(album.name) " +
                                        "END = LOWER('" + rowalbum.replace("'", "''") +
                                        "') AND CASE WHEN artist.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(artist.prefix, ' ', artist.name)) ELSE LOWER(artist.name) " +
                                        "END = LOWER('" + rowartist.replace("'", "''") + "');")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                if not tmpalbum:
                                    tmpalbum = str(rows[0])
                                if not tmpartist:
                                    tmpartist = str(rows[1])
                        if (not tmpsong or not tmpalbum) and (trackmbid and albummbid):
                            tmpquery = ("SELECT `song`.`id`, `album`.`id` " +
                                        "FROM `song` INNER JOIN album ON `song`.`album` = `album`.`id` " +
                                        "WHERE `song`.`mbid` = '" + trackmbid + "' AND " +
                                        "`album`.`mbid` = '" + albummbid + "';")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                if not tmpsong:
                                    tmpsong = str(rows[0])
                                if not tmpalbum:
                                    tmpalbum = str(rows[1])
                        if (not tmpsong or not tmpalbum) and (rowtrack and rowalbum):
                            tmpquery = ("SELECT `song`.`id`, `album`.`id` " +
                                        "FROM `song` INNER JOIN album ON `song`.`album` = `album`.`id` " +
                                        "WHERE LOWER(`song`.`title`) = LOWER('" + rowtrack.replace("'", "''") +
                                        "') AND CASE WHEN album.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(album.prefix, ' ', album.name)) ELSE LOWER(album.name) " +
                                        "END = LOWER('" + rowalbum.replace("'", "''") + "');")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                if not tmpsong:
                                    tmpsong = str(rows[0])
                                if not tmpalbum:
                                    tmpalbum = str(rows[1])
                        # print rows with missing data so i can check
                        if not tmpsong or not tmpartist or not tmpalbum:
                            print(str(row[0]), '\t', str(row[1]), '\t', str(row[2]), '\t', str(row[3]),
                                  '\t', str(row[4]), '\t', str(row[5]), '\t', str(row[6]),
                                  '\t', tmpsong, '\t', tmpartist, '\t', tmpalbum)
                        # try individuals
                        if not tmpsong:
                            tmpquery = ("SELECT `id` FROM `song` WHERE LOWER(`title`) = LOWER('" + rowtrack.replace("'", "''") +
                                        "') AND artist in (SELECT id from artist WHERE CASE when prefix IS NOT NULL " +
                                        "THEN LOWER(CONCAT(prefix, ' ', name)) ELSE LOWER(name) END = LOWER('" +
                                        rowartist.replace("'", "''") + "')) AND " +
                                        "album in (SELECT id from album WHERE LOWER(`name`) = LOWER('" +
                                        rowalbum.replace("'", "''") + "'));")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                tmpsong = rows[0]

                        # Check for the album
                        if not tmpalbum:
                            tmpquery = ("SELECT `id` FROM `album` WHERE LOWER(`name`) = LOWER('" + rowalbum.replace("'", "''") +
                                        "') AND id in (SELECT album from song WHERE LOWER(`title`) = LOWER('" +
                                        rowtrack.replace("'", "''") +
                                        "')) AND album_artist in (SELECT id from artist WHERE LOWER(`name`) = LOWER('" +
                                        rowartist.replace("'", "''") + "'));")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                tmpalbum = rows[0]
                        # search ampache db for artist
                        if not tmpartist:
                            tmpquery = ("SELECT `id` FROM `artist` WHERE CASE WHEN artist.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(artist.prefix, ' ', artist.name)) ELSE LOWER(artist.name) " +
                                        "END = LOWER('" + rowartist.replace("'", "''") + "')")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                tmpartist = rows[0]
                    # search ampache using mbid for artist
                    if not tmpartist and artistmbid:
                            tmpquery = ("SELECT `id` FROM `artist` WHERE `mbid` = '" + artistmbid + "'")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                tmpartist = rows[0]
                    # search ampache using name for artist
                    if not tmpartist and row[2]:
                            tmpquery = ("SELECT `id` FROM `artist` WHERE CASE WHEN artist.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(artist.prefix, ' ', artist.name)) ELSE LOWER(artist.name) " +
                                        "END = LOWER('" + rowartist.replace("'", "''") + "')")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                tmpartist = rows[0]
                    # If you're still missing the album but have an artist and a song try to find single album matches
                    # It's possible for last.fm exports to be missing album details especially for older plays
                    if not tmpalbum:
                        # search ampache using mbid for album
                        if albummbid and trackmbid:
                            tmpquery = ("SELECT `id` FROM `album` WHERE `mbid` = '" + albummbid + "' AND " +
                                        "id in (SELECT album from song WHERE `mbid` = '" + trackmbid +
                                        "');")
                            # Check the database
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                tmpalbum = rows[0]
                        # check using text instead
                        if not tmpalbum and tmpartist:
                            tmpcount = 0
                            tmpquery = ("SELECT `id` FROM `album` WHERE `id` in (SELECT `album` FROM `song` WHERE LOWER(`title`) = LOWER('" +
                                        rowtrack.replace("'", "''") + "')) AND album_artist in " +
                                        "(SELECT id from artist WHERE `id` = '" + str(tmpartist) + "');")
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                # Don't allow multiple albums
                                if tmpcount == 0:
                                    tmpalbum = rows[0]
                                else:
                                    tmpalbum = None
                                tmpcount += 1
                        # Look for albums under "Various Artist" tags
                        if not tmpalbum:
                            tmpcount = 0
                            tmpquery = ("SELECT `id` FROM `album` WHERE `id` in (SELECT `album` FROM `song` WHERE LOWER(`title`) = LOWER('" +
                                        rowtrack.replace("'", "''") + "')) AND album_artist in " +
                                        "(SELECT id from artist WHERE `name` = 'Various Artists');")
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                # Don't allow multiple albums
                                if tmpcount == 0:
                                    tmpalbum = rows[0]
                                else:
                                    tmpalbum = None
                                tmpcount += 1
                            # if you get a various artist you will want the song artist instead
                            tmpcount = 0
                            tmpquery = ("SELECT `id` FROM `artist` WHERE CASE WHEN artist.prefix IS NOT NULL THEN " +
                                        "LOWER(CONCAT(artist.prefix, ' ', artist.name)) ELSE LOWER(artist.name) " +
                                        "END = LOWER('" + rowartist.replace("'", "''") + "')")
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                # Don't allow multiple albums
                                if tmpcount == 0:
                                    tmpartist = rows[0]
                                else:
                                    tmpartist = None
                                tmpcount += 1
                        # If you find the missing album you need to check for the song again as well
                        if tmpalbum:
                            tmpquery = ("SELECT `id` FROM `song` WHERE LOWER(`title`) = LOWER('" + rowtrack.replace("'", "''") +
                                        "') AND album in (SELECT id from album WHERE `id` = '" +
                                        str(tmpalbum) + "');")
                            try:
                                cursor.execute(tmpquery)
                            except mysql.connector.errors.ProgrammingError:
                                print('ERROR WITH QUERY:\n' + tmpquery)
                            for rows in cursor:
                                tmpsong = rows[0]

                    # if you find all three values in the database
                    # we have an exact match
                    if tmpsong and tmpalbum and tmpartist:

                        # ampache creates a separate row for
                        # song/album/artist for each play
                        removeoldplay = ("DELETE FROM `" + dbname + "`.`object_count` " +
                                          "WHERE date = " + tmpdate + " AND user = " + myid + ";")

                        # make sure the track is set to played in the song table
                        setplayed = ("UPDATE `song` SET `played` = 1" +
                                     " WHERE `id` = " + str(tmpsong) +
                                     " AND `played` = 0;")

                        # Queries to insert data
                        insertsong = ("INSERT INTO `" + dbname + "`.`object_count` " +
                                      "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                                      " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                                      "VALUES (0, 'song', " + str(tmpsong) + ", " + tmpdate + ", " +
                                      myid + "," + " 'mysql-connection', NULL, NULL, NULL, 'stream');")
                        insertalbum = ("INSERT INTO `" + dbname + "`.`object_count` " +
                                       "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                                       " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                                       "VALUES (0, 'album', " + str(tmpalbum) + ", " + tmpdate + ", " +
                                       myid + "," + " 'mysql-connection', NULL, NULL, NULL, 'stream');")
                        insertartist = ("INSERT INTO `" + dbname + "`.`object_count` " +
                                        "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                                        " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                                        "VALUES (0, 'artist', " + str(tmpartist) + ", " + tmpdate + ", " +
                                        myid + "," + " 'mysql-connection', NULL, NULL, NULL, 'stream');")

                        # remove old play
                        tmpcursor = cnx.cursor()
                        tmpcursor.execute(removeoldplay)

                        # insert new play
                        tmpcursor.execute(insertsong)
                        tmpcursor.execute(insertalbum)
                        tmpcursor.execute(insertartist)

                        # make sure song is set to played
                        tmpcursor.execute(setplayed)

    # Clear songs of played status if they haven't been played
    cursor.execute(clearnotplayed)
    # close connections
    csvfile.close()
    cnx.close()
else:
    print('unable to connect to database')
