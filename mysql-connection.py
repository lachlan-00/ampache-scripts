#!/usr/bin/env python3

""" mysql dump into ampache from tsv

  merge last.fm data with ampache
  -------------------------------

  This script will examine a dump file from lastscrape
  then query your ampache database

  if it matches the artist, album and song
  it will update your databse to reflect each play

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
# the song table stores artist/album as id (from artist/album tables)
songquery = "SELECT `id` FROM `song` WHERE "
# get ID for song table
albumquery = "SELECT `id` FROM `album` WHERE "
artistquery = "SELECT `id` FROM `artist` WHERE "

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
            for row in openfile:
                tmprow = []
                tmpartist = None
                tmpalbum = None
                tmpsong = None
                founddate = False
                foundartist = None
                foundalbum = None
                foundsong = None
                try:
                    test = row[0]
                except IndexError:
                    test = None
                if test:
                    # search ampache db for song, album and artist
                    # Look for a musicbrainz ID before the text of the tags
                    # This should hopefully be more reliable if your tags change a lot
                    if row[4] and row[6] and row[5]:
                        tmpquery = (songquery + "`mbid` = '" + row[4].replace("'", "''") + "' AND artist in " +
                                    "(SELECT id from artist WHERE `id` = '" + row[5].replace("'", "''") + "') AND " +
                                    "album in (SELECT id from album WHERE `id` = '" + row[6].replace("'", "''") +
                                    "');")
                        # Check the database
                        try:
                            cursor.execute(tmpquery)
                        except mysql.connector.errors.ProgrammingError:
                            print('ERROR WITH QUERY:\n' + tmpquery)
                        for rows in cursor:
                            tmpsong = rows[0]
                    # search ampache db for album
                    if row[6] and row[4]:
                        tmpquery = (albumquery + "`mbid` = '" + row[6].replace("'", "''") + "' AND " +
                                    "id in (SELECT album from song WHERE `title` = '" + row[1].replace("'", "''") +
                                    "') AND album_artist in (SELECT id from artist WHERE `id` = '" +
                                    row[4].replace("'", "''") + "');")
                        # Check the database
                        try:
                            cursor.execute(tmpquery)
                        except mysql.connector.errors.ProgrammingError:
                            print('ERROR WITH QUERY:\n' + tmpquery)
                        for rows in cursor:
                            tmpalbum = rows[0]
                    # search ampache db for artist
                    if row[5]:
                        tmpquery = (artistquery + "`mbid` = '" + row[5].replace("'", "''") + "'")
                        # Check the database
                        try:
                            cursor.execute(tmpquery)
                        except mysql.connector.errors.ProgrammingError:
                            print('ERROR WITH QUERY:\n' + tmpquery)
                        for rows in cursor:
                            tmpartist = rows[0]
                    # find missing data if the mbid didn't work
                    if not tmpsong:
                        tmpquery = (songquery + "`title` = '" + row[1].replace("'", "''") + "' AND artist in " +
                                    "(SELECT id from artist WHERE `name` = '" + row[2].replace("'", "''") + "') AND " +
                                    "album in (SELECT id from album WHERE `name` = '" + row[3].replace("'", "''") +
                                    "');")
                        # Check the database
                        try:
                            cursor.execute(tmpquery)
                        except mysql.connector.errors.ProgrammingError:
                            print('ERROR WITH QUERY:\n' + tmpquery)
                        for rows in cursor:
                            tmpsong = rows[0]
                    # Check for the album
                    if not tmpalbum:
                        tmpquery = (albumquery + "`name` = '" + row[3].replace("'", "''") + "' AND " +
                                    "id in (SELECT album from song WHERE `title` = '" + row[1].replace("'", "''") +
                                    "') AND album_artist in (SELECT id from artist WHERE `name` = '" +
                                    row[2].replace("'", "''") + "');")
                        # Check the database
                        try:
                            cursor.execute(tmpquery)
                        except mysql.connector.errors.ProgrammingError:
                            print('ERROR WITH QUERY:\n' + tmpquery)
                        for rows in cursor:
                            tmpalbum = rows[0]
                    # search ampache db for artist
                    if not tmpartist:
                        tmpquery = (artistquery + "`name` = '" + row[2].replace("'", "''") + "'")
                        # Check the database
                        try:
                            cursor.execute(tmpquery)
                        except mysql.connector.errors.ProgrammingError:
                            print('ERROR WITH QUERY:\n' + tmpquery)
                        for rows in cursor:
                            tmpartist = rows[0]

                    # if you find all three values in the database
                    # we have an exact match
                    if tmpsong and tmpalbum and tmpartist:

                        tmpcursor = cnx.cursor()

                        # ampache creates a separate row for 
                        # song/album/artist for each play
                        checkforplay = ("SELECT date, object_type, object_id " +
                                        "from object_count " +
                                        "WHERE date = " + str(row[0]) + " AND " +
                                        "user = " + myid)

                        # make sure the track is set to played in the song table
                        setplayed = ("UPDATE `song` SET `played` = 1" +
                                     " WHERE `id` = " + str(tmpsong) +
                                     " AND `played` = 0;")

                        tmpcursor.execute(checkforplay)

                        for rows in tmpcursor:
                            if str(row[0]) in rows:
                                founddate = True
                            if tmpsong in rows:
                                foundsong = True
                            if tmpalbum in rows:
                                foundalbum = True
                            if tmpartist in rows:
                                foundartist = True

                        # database already has this play recorded
                        if foundsong and foundalbum and foundartist:
                            tmpcursor.execute(setplayed)
                            pass
                        # database is missing this play completely
                        elif not foundsong and not foundalbum and not foundartist and not founddate:
                            print('\ninserting row')
                            print(row)
                            tmpincursor = cnx.cursor()
                            insertsong = ("INSERT INTO `" + dbname + "`.`object_count` " +
                                          "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                                          " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                                          "VALUES ('0', 'song', '" + str(tmpsong) + "', '" + row[0] + "', '" +
                                          myid + "'," + " NULL, NULL, NULL, NULL, 'stream');")
                            tmpincursor.execute(insertsong)
                            insertalbum = ("INSERT INTO `" + dbname + "`.`object_count` " +
                                           "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                                           " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                                           "VALUES ('0', 'album', '" + str(tmpalbum) + "', '" + row[0] + "', '" +
                                           myid + "'," + " NULL, NULL, NULL, NULL, 'stream');")
                            tmpincursor.execute(insertalbum)
                            insertartist = ("INSERT INTO `" + dbname + "`.`object_count` " +
                                            "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                                            " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                                            "VALUES ('0', 'artist', '" + str(tmpartist) + "', '" + row[0] + "', '" +
                                            myid + "'," + " NULL, NULL, NULL, NULL, 'stream');")
                            tmpincursor.execute(insertartist)
                            tmpincursor.execute(setplayed)
                        # Found the date but not the right song
                        elif founddate and not (foundsong and foundalbum and foundartist):
                            print('\nupdating row')
                            print(row)
                            tmpupcursor = cnx.cursor()
                            updatesong = ("UPDATE `" + dbname + "`.`object_count` " +
                                          "SET `object_id` = '" + str(tmpsong) + "' WHERE " +
                                          "date = " + row[0] + "' AND object_type = 'artist'" +
                                          "`user` = " + myid + ";")
                            tmpupcursor.execute(updatesong)
                            updatealbum = ("UPDATE `" + dbname + "`.`object_count` " +
                                           "SET `object_id` = '" + str(tmpalbum) + "' WHERE " +
                                           "date = " + row[0] + "' AND object_type = 'artist'" +
                                           "`user` = " + myid + ";")
                            tmpupcursor.execute(updatealbum)
                            updateartist = ("UPDATE `" + dbname + "`.`object_count` " +
                                            "SET `object_id` = '" + str(tmpartist) + "' WHERE " +
                                            "date = " + row[0] + "' AND object_type = 'artist'" +
                                            "`user` = " + myid + ";")
                            tmpupcursor.execute(updateartist)
                            tmpupcursor.execute(setplayed)

                        # partial match means it didn't get everything we needed
                        else:
                            notfoundlist.append(row)
                    # If you don't find all 3 you don't have an exact match
                    # so don't add these track to the database
                    else:
                        notfoundlist.append(row)
                        notfoundcount += 1
    # close connections
    csvfile.close()
    cnx.close()
else:
    print('unable to connect to database')

# print tracks that couldn't be integrated into the database
for row in notfoundlist:
    print(row)
print(str(notfoundcount))
