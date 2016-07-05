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
dumpfile = None
lovedfile = None
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
    dbuser='username'
    dbpass='password'
    dbhost='127.0.0.1'
    dbname='database'

    # Ampache variables
    myid = '2'


# object_count will store each play for tracks
# with separate rows for artist & album
selectquery = "SELECT * FROM `object_count` ORDER BY `id` DESC"
# the song table stores artist/album as id (from artist/album tables)
songquery = "SELECT `id` FROM `song` WHERE `title` = '"
# get ID for song table
albumquery = "SELECT `id` FROM `album` WHERE `name` = '"
artistquery = "SELECT `id` FROM `artist` WHERE `name` = '"


notfoundcount = 0
notfoundlist = []

cnx = None

try:
    cnx = mysql.connector.connect(user = dbuser, password = dbpass,
                                  host = dbhost, database= dbname)
except mysql.connector.errors.InterfaceError:
    try:
        cnx = mysql.connector.connection.MySQLConnection(user = dbuser,
                                                         password = dbpass,
                                                         host = dbhost,
                                                         database = dbname)
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
            openfile = reversed(list(csv.reader(csvfile, delimiter='\t',)))
            for row in openfile:
                tmprow = []
                tmpartist = None
                tmpalbum = None
                tmpsong = None
                foundartist = None
                foundalbum = None
                foundsong = None
                try:
                    test = row[0]
                except IndexError:
                    test = None
                if test:
                    # search ampache db for song
                    tmpquery = (songquery + row[1].replace("'", "''") + "'")
                    try:
                        cursor.execute(tmpquery)
                    except mysql.connector.errors.ProgrammingError:
                        print('ERROR WITH QUERY:\n' + tmpquery)
                    for rows in cursor:
                        tmpsong = rows[0]
                    # search ampache db for album
                    tmpquery = (albumquery + row[3].replace("'", "''") + "'")
                    try:
                        cursor.execute(tmpquery)
                    except mysql.connector.errors.ProgrammingError:
                        print('ERROR WITH QUERY:\n' + tmpquery)
                    for rows in cursor:
                        tmpalbum = rows[0]
                    # search ampache db for artist
                    tmpquery = (artistquery + row[2].replace("'", "''") + "'")
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
                        checksong = ("SELECT * FROM `object_count` WHERE " +
                                     "`object_type` = 'song' AND `date` = '" +
                                     str(row[0]) + "' AND `object_id` = '" +
                                     str(tmpsong) + "';")
                        checkalbum = ("SELECT * FROM `object_count` WHERE" +
                                      " `object_type` = 'album' AND `date` =" +
                                      " '" + str(row[0]) + "' AND `object" +
                                      "_id` = '" +
                                      str(tmpalbum) + "';")
                        checkartist = ("SELECT * FROM `object_count` WHERE" +
                                       " `object_type` = 'artist' AND `date`" +
                                       " = '" + str(row[0]) + "' AND `obje" +
                                       "ct_id` = '" +
                                       str(tmpartist) + "';")

                        # make sure the track is set to played in the song table
                        setplayed = ("UPDATE `song` SET `played` = 1" +
                                     " WHERE `id` = " + str(tmpsong) +
                                     " AND `played` = 0;")
                        tmpcursor.execute(setplayed)

                        tmpcursor.execute(checksong)
                        for rows in tmpcursor:
                            if tmpsong in rows:
                                foundsong = True
                        tmpcursor.execute(checkalbum)
                        for rows in tmpcursor:
                            if tmpalbum in rows:
                                foundalbum = True
                        tmpcursor.execute(checkartist)
                        for rows in tmpcursor:
                            if tmpartist in rows:
                                foundartist = True
                        # database already has this play recorded
                        if foundsong and foundalbum and foundartist:
                            pass
                        # database is missing this play
                        elif not foundsong and not foundalbum and not foundartist:
                            tmpincursor = cnx.cursor()
                            insertsong = ("INSERT INTO `" + dbname +"`.`object_count` " +
                               "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                               " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                               "VALUES ('0', 'song', '" + str(tmpsong) + "', '" + row[0] + "', '2'," +
                               " NULL, NULL, NULL, NULL, 'stream');")
                            tmpincursor.execute(insertsong)
                            insertalbum = ("INSERT INTO `" + dbname +"`.`object_count` " +
                               "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                               " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                               "VALUES ('0', 'album', '" + str(tmpalbum) + "', '" + row[0] + "', '2'," +
                               " NULL, NULL, NULL, NULL, 'stream');")
                            tmpincursor.execute(insertalbum)
                            insertartist = ("INSERT INTO `" + dbname +"`.`object_count` " +
                               "(`id`, `object_type`, `object_id`, `date`, `user`, `agent`," +
                               " `geo_latitude`, `geo_longitude`, `geo_name`, `count_type`) " +
                               "VALUES ('0', 'artist', '" + str(tmpartist) + "', '" + row[0] + "', '2'," +
                               " NULL, NULL, NULL, NULL, 'stream');")
                            tmpincursor.execute(insertartist)
                         #partial match means it didn't get everything we needed
                        else:
                            notfoundlist.append(row)
                    # If you don't find all 3 you don't have an exact match
                    # so don't add these track to the database
                    else:
                        notfoundlist.append(row)
                        notfoundcount = notfoundcount + 1
    # close connections
    csvfile.close()
    cnx.close()
else:
    print('unable to connect to database')

# print tracks that couldn't be integrated into the database
for row in notfoundlist:
    print(row)
print(str(notfoundcount))
