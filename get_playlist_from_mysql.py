#!/usr/bin/env python3

""" query ampache to create playlist files

  query ampache database for top rated songs
  ------------------------------------------

  This script will query your ampache database
  and create output m3u playlists for use in other
  programs like rhythmbox.

"""


import codecs
import csv
import mimetypes
import os
import time
import mysql.connector

FINDPATH = '/mnt/music/'
REPLACEPATH = '/home/user/Music/'
FOLDERPATH = '/home/user/Music/playlists/'
SETTINGS = 'settings.csv'
REPLACE = ('%', "#", ';', '"', '<', '>', '?', '[', '\\', "]", '^', '`', '{',
           '|', ')', '€', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹',
           'Œ', 'Ž', '‘', '’', '“', '”', '•', '–', '—', '˜', '™', 'š', '›',
           'œ', 'ž', 'Ÿ', '¡', '¢', '£', '¥', '|', '§', '¨', '©', 'ª', '«',
           '¬', '¯', '®', '¯', '°', '±', '²', '³', '´', 'µ', '¶', '·', '¸',
           '¹', 'º', '»', '¼', '½', '¾', '¿', 'À', 'Á', 'Â', 'Ã', 'Ä', 'Å',
           'Æ', 'Ç', 'È', 'É', 'Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ', 'Ò',
           'Ó', 'Ô', 'Õ', 'Ö', 'Ø', 'Ù', 'Ú', 'Û', 'Ü', 'Ý', 'Þ', 'ß', 'à',
           'á', 'â', 'ã', 'ä', 'å', 'æ', 'ç', 'è', 'é', 'ê', 'ë', 'ì', 'í',
           'î', 'ï', 'ð', 'ñ', 'ò', 'ó', 'ô', 'õ', 'ö', '÷', 'ø', 'ù', 'ú',
           'û', 'ü', 'ý', 'þ', 'ÿ', '¦', ':', '*', '<<')

# Database connector and details
cnx = None
dbuser = None
dbpass = None
dbhost = None
dbname = None
myid = None

# get settings for database
if not os.path.isfile(SETTINGS):
    SETTINGS = os.path.join(os.path.dirname(os.path.relpath(__file__)), SETTINGS)
if not os.path.isfile(SETTINGS):
    SETTINGS = os.path.join(os.path.dirname(os.path.realpath(__file__)), SETTINGS)
if os.path.isfile(SETTINGS):
    print('found settings file')
    with open(SETTINGS, 'r') as csvfile:
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
                    FINDPATH = row[1]
                elif row[0] == 'replace':
                    REPLACEPATH = row[1]
                elif row[0] == 'outfolder':
                    FOLDERPATH = row[1]
    csvfile.close()
else:
    # Database variables
    dbuser = 'username'
    dbpass = 'password'
    dbhost = '127.0.0.1'
    dbname = 'database'

    # Ampache variables
    myid = '2'

QUERIES = (("SELECT REPLACE(song.file, '" + FINDPATH + "', '" + REPLACEPATH + "'), artist.name, album.name, song.title " +
            "FROM song " +
            "INNER JOIN artist on song.artist = artist.id INNER JOIN album on song.album = album.id " +
            "WHERE song.id in (SELECT rating.object_id FROM rating WHERE rating.object_type = 'song' and rating.user = " + str(myid) + " and rating.rating = 5) " +
            "ORDER BY song.file",
            FOLDERPATH + '5star.m3u'),

           ("SELECT REPLACE(song.file, '" + FINDPATH + "', '" + REPLACEPATH + "'), artist.name, album.name, song.title " +
            "FROM song " +
            "INNER JOIN artist on song.artist = artist.id INNER JOIN album on song.album = album.id " +
            "WHERE song.id in (SELECT rating.object_id FROM rating WHERE rating.object_type = 'song' and rating.user = " + str(myid) + " and rating.rating = 4) " +
            "ORDER BY song.file",
            FOLDERPATH + '4star.m3u'),

           ("SELECT REPLACE(song.file, '" + FINDPATH + "', '" + REPLACEPATH + "'), artist.name, album.name, song.title " +
            "FROM song " +
            "INNER JOIN artist on song.artist = artist.id INNER JOIN album on song.album = album.id " +
            "WHERE song.id in (SELECT rating.object_id FROM rating WHERE rating.object_type = 'song' and rating.user = " + str(myid) + " and rating.rating = 3) " +
            "ORDER BY song.file",
            FOLDERPATH + '3star.m3u'),

           ("SELECT REPLACE(song.file, '" + FINDPATH + "', '" + REPLACEPATH + "'), artist.name, album.name, song.title " +
            "FROM song " +
            "INNER JOIN artist on song.artist = artist.id INNER JOIN album on song.album = album.id " +
            "WHERE song.id in (SELECT rating.object_id FROM rating WHERE rating.object_type = 'song' and rating.user = " + str(myid) + " and rating.rating = 2) " +
            "ORDER BY song.file",
            FOLDERPATH + '2star.m3u'),

           ("SELECT REPLACE(song.file, '" + FINDPATH + "', '" + REPLACEPATH + "'), artist.name, album.name, song.title " +
            "FROM song " +
            "INNER JOIN artist on song.artist = artist.id INNER JOIN album on song.album = album.id " +
            "WHERE song.id in (SELECT rating.object_id FROM rating WHERE rating.object_type = 'song' and rating.user = " + str(myid) + " and rating.rating = 1) " +
            "ORDER BY song.file",
            FOLDERPATH + '1star.m3u'),

           ("SELECT REPLACE(song.file, '" + FINDPATH + "', '" + REPLACEPATH + "'), artist.name, album.name, song.title " +
            "FROM song " +
            "INNER JOIN artist on song.artist = artist.id INNER JOIN album on song.album = album.id " +
            "WHERE song.album NOT IN (SELECT rating.object_id FROM rating WHERE rating.object_type = 'album' and rating.user = " + str(myid) + " and rating.rating IN (1, 2, 3, 4, 5)) " +
            "AND song.album IN (SELECT object_count.object_id FROM object_count WHERE object_count.object_type = 'album' and object_count.user = " + str(myid) + ") " +
            "ORDER BY song.file",
            FOLDERPATH + 'unrated-albums.m3u'),

           ("SELECT REPLACE(`file`, '" + FINDPATH + "', '" + REPLACEPATH + "'), artist.name, album.name, song.title " +
            "FROM song  " +
            "INNER JOIN artist on song.artist = artist.id " +
            "INNER JOIN album on song.album = album.id " +
            "WHERE song.album NOT IN (SELECT rating.object_id  " +
            "FROM rating " +
            "WHERE rating.object_type = 'album' and " +
            "rating.user = " + str(myid) + " and " +
            "rating.rating IN (3, 4, 5) ) AND " +
            "song.album IN (SELECT album.id " +
            "FROM rating " +
            "INNER JOIN song on song.id = rating.object_id " +
            "INNER JOIN album on song.album = album.id " +
            "WHERE rating.object_type = 'song' and " +
            "rating.user = " + str(myid) + " and " +
            "rating.rating = 2) " +
            "ORDER BY song.`file`",
            FOLDERPATH + '2ndchancealbums.m3u'))

def checkdbconn(cnx):
    """ Maintain database connection """
    if cnx:
        # Check existing connection
        if cnx.is_connected():
            return
        if not cnx.is_connected():
            print('\nError: Reconnecting to database\n')
            cnx.reconnect(attempts=4, delay=4)
            return
    if not cnx:
        time.sleep(5)
        # Create a new DB connection
        print('\nCreating Database connection\n')
        try:
            cnx = mysql.connector.connect(user=dbuser, password=dbpass,
                                               host=dbhost, database=dbname, connection_timeout=15)
            print('Connected')
        except mysql.connector.errors.InterfaceError:
            try:
                cnx = mysql.connector.connection.MySQLConnection(user=dbuser,
                                                                      password=dbpass,
                                                                      host=dbhost,
                                                                      database=dbname, connection_timeout=15)
                print('Connected')
            except mysql.connector.errors.InterfaceError:
                pass
    #
    # Try to get through with ssh fowarding
    #
    # eg. ssh -L 3306:localhost:3306 externalhost
    #
    if not cnx:
        print("Trying localhost DB connections")
        try:
            cnx = mysql.connector.connect(user=dbuser, password=dbpass,
                                               host='127.0.0.1', database=dbname, connection_timeout=15)
            print('Connected')
        except mysql.connector.errors.InterfaceError:
            pass
    return cnx


# Write to log file
def log_processing(logfile, logmessage):
    """ Perform log operations """
    # Create if missing
    if not os.path.exists(logfile):
        print('creating')
        logname = codecs.open(logfile, "w", "utf8")
        logname.close()
    logname = codecs.open(logfile, "a", "utf8")
    try:
        logline = [logmessage]
        logname.write((u"".join(logline)) + u"\n")
    except UnicodeDecodeError:
        print('LOG UNICODE ERROR')
        logline = [logmessage.decode('utf-8')]
        logname.write((u"".join(logline)) + u"\n")
    logname.close()

# Connect to the database
cnx = checkdbconn(cnx)

# process query and copy results to the destination
if cnx:
    print('Connection Established\n')
    for playlists in QUERIES:
        outfile = playlists[1]
        tmpquery = playlists[0]
        print('Creating: ' + outfile)
        try:
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
        except mysql.connector.errors.ProgrammingError:
            print('ERROR WITH QUERY:\n' + tmpquery)
        except BrokenPipeError:
            checkdbconn(cnx)
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
            pass
        except ConnectionResetError:
            checkdbconn(cnx)
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
            pass
        except mysql.connector.errors.OperationalError:
            checkdbconn(cnx)
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
            pass
        except mysql.connector.errors.InterfaceError:
            checkdbconn(cnx)
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
            pass




        try:
            files = codecs.open(outfile, "w", "utf8")
            files.close()
        except FileNotFoundError:
            print('FileNotFound:\n    ' + outfile)
            pass
        tmpcount = 0
        outcount = 0
        try:
            for rows in cursor:
                if os.path.isfile(outfile):
                    if outcount == 0:
                        log_processing(outfile, '#EXTM3U')
                        outcount = outcount + 1
                    else:
                        files = rows[0]
                        artist = rows[1]
                        album = rows[2]
                        song = rows[3]
                        song_line = ('#EXTINF:,' + song)
                        log_processing(outfile, song_line)
                        log_processing(outfile, files)
                        outcount = outcount + 1
        except mysql.connector.errors.ProgrammingError:
            print('ERROR WITH QUERY:\n' + tmpquery)
        except BrokenPipeError:
            cnx = checkdbconn(cnx)
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
            for rows in cursor:
                if os.path.isfile(outfile):
                    if outcount == 0:
                        log_processing(outfile, '#EXTM3U')
                        outcount = outcount + 1
                    else:
                        files = rows[0]
                        artist = rows[1]
                        album = rows[2]
                        song = rows[3]
                        song_line = ('#EXTINF:,' + song)
                        log_processing(outfile, song_line)
                        log_processing(outfile, files)
                        outcount = outcount + 1
            pass
        except ConnectionResetError:
            cnx = checkdbconn(cnx)
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
            for rows in cursor:
                if os.path.isfile(outfile):
                    if outcount == 0:
                        log_processing(outfile, '#EXTM3U')
                        outcount = outcount + 1
                    else:
                        files = rows[0]
                        artist = rows[1]
                        album = rows[2]
                        song = rows[3]
                        song_line = ('#EXTINF:,' + song)
                        log_processing(outfile, song_line)
                        log_processing(outfile, files)
                        outcount = outcount + 1
            pass
        except mysql.connector.errors.OperationalError:
            cnx = checkdbconn(cnx)
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
            for rows in cursor:
                if os.path.isfile(outfile):
                    if outcount == 0:
                        log_processing(outfile, '#EXTM3U')
                        outcount = outcount + 1
                    else:
                        files = rows[0]
                        artist = rows[1]
                        album = rows[2]
                        song = rows[3]
                        song_line = ('#EXTINF:,' + song)
                        log_processing(outfile, song_line)
                        log_processing(outfile, files)
                        outcount = outcount + 1
            pass
        except mysql.connector.errors.InterfaceError:
            cnx = checkdbconn(cnx)
            cursor = cnx.cursor(buffered=True)
            cursor.execute(tmpquery)
            for rows in cursor:
                if os.path.isfile(outfile):
                    if outcount == 0:
                        log_processing(outfile, '#EXTM3U')
                        outcount = outcount + 1
                    else:
                        files = rows[0]
                        artist = rows[1]
                        album = rows[2]
                        song = rows[3]
                        song_line = ('#EXTINF:,' + song)
                        log_processing(outfile, song_line)
                        log_processing(outfile, files)
                        outcount = outcount + 1
            pass
else:
    print('Unable to generate connection to database')
