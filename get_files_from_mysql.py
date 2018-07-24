#!/usr/bin/env python3

""" get ampache files from mysql

  query ampache database for top rated songs to copy
  --------------------------------------------------

  This script will query your ampache database for top rated songs
  if can find the song by it's path it will copy to your destination

"""


import csv
import mimetypes
import os
import shutil
import sys
import time
import mysql.connector


SETTINGS = 'settings.csv'
REPLACE = ('%', "#", ';', '"', '<', '>', '?', '[', '\\', "]", '^', '`', '{',
           '|', '}', '€', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹',
           'Œ', 'Ž', '‘', '’', '“', '”', '•', '–', '—', '˜', '™', 'š', '›',
           'œ', 'ž', 'Ÿ', '¡', '¢', '£', '¥', '|', '§', '¨', '©', 'ª', '«',
           '¬', '¯', '®', '¯', '°', '±', '²', '³', '´', 'µ', '¶', '·', '¸',
           '¹', 'º', '»', '¼', '½', '¾', '¿', 'À', 'Á', 'Â', 'Ã', 'Ä', 'Å',
           'Æ', 'Ç', 'È', 'É', 'Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ', 'Ò',
           'Ó', 'Ô', 'Õ', 'Ö', 'Ø', 'Ù', 'Ú', 'Û', 'Ü', 'Ý', 'Þ', 'ß', 'à',
           'á', 'â', 'ã', 'ä', 'å', 'æ', 'ç', 'è', 'é', 'ê', 'ë', 'ì', 'í',
           'î', 'ï', 'ð', 'ñ', 'ò', 'ó', 'ô', 'õ', 'ö', '÷', 'ø', 'ù', 'ú',
           'û', 'ü', 'ý', 'þ', 'ÿ', '¦', ':', '*', '<<', '...')

# Database connector and details
cnx = None
dbuser = None
dbpass = None
dbhost = None
dbname = None

# destination folder
destination = None
depth = 0
# files that should be in the destination
destinfiles = []


def foldersearch(input_string):
    """ process dirs or run tag check for files (if mp3) """
    if os.path.isdir(input_string):
        current_path = os.listdir(input_string)
        # alphabetically
        # current_path.sort(key=lambda y: y.lower())
        # sort by most recent modification date
        current_path.sort(key=lambda s: os.path.getmtime(os.path.join(input_string, s)), reverse=True)
        for pathfiles in current_path:
            tmppath = os.path.join(input_string, pathfiles)
            if os.path.isdir(tmppath):
                foldersearch(tmppath)
            elif os.path.isfile(tmppath):
                # check mimetype for mp3 file
                if mimetypes.guess_type(tmppath)[0] == 'audio/mpeg':
                    # run filesearch
                    filecheck(tmppath)
                    # print(tmppath)


def filecheck(input_string):
    if input_string not in destinfiles and input_string != '.is_audio_player':
        print(input_string, ' does not belong here!')
        os.remove(input_string)


# destination need to be an argument in the form of
# /d:$PATH
for arguments in sys.argv:
    if arguments[:3].lower() == '/d:':
        destination = arguments[3:]
    if arguments[:3].lower() == '/f:':
        depth = int(arguments[3:])

# get settings for database
if not os.path.isfile(SETTINGS):
    SETTINGS = os.path.join(os.path.dirname(os.path.realpath(__file__)), SETTINGS)
if os.path.isfile(SETTINGS):
    print('Loading local settings file\n')
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
    csvfile.close()
else:
    # Database variables
    dbuser = 'username'
    dbpass = 'password'
    dbhost = '127.0.0.1'
    dbname = 'database'

    # Ampache variables
    myid = '2'

if destination:
    # only continue if output is correct
    if not os.path.isdir(destination):
        destination = None
    # Connect to the mysql database
    time.sleep(5)
    print('creating database connection\n')
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
            print("db connection fail")
            pass
else:
    print('\ndestination not found. use /d: to set an output path\n' +
          '\n   e.g. /media/user/USB/Music\n')

# process query and copy results to the destination
if cnx and destination:
    print('Connection Established\n')
    cursor = cnx.cursor()
    tmpquery = ("SELECT song.file, artist.name, album.name, song.title " +
                 "FROM rating " +
                 "INNER JOIN song on rating.object_id = song.id AND rating.object_type = 'song' AND rating.user = " + str(self.myid) + " " +
                 "INNER JOIN artist on song.artist = artist.id " +
                 "INNER JOIN album on song.album = album.id " +
                 "WHERE song.id in (SELECT object_id FROM `rating` " +
                 "                  WHERE object_type = 'song' and user = " + str(self.myid) + " AND " +
                 "                        rating in (3,4,5))")
    try:
        cursor.execute(tmpquery)
    except mysql.connector.errors.ProgrammingError:
        print('ERROR WITH QUERY:\n' + tmpquery)
    for rows in cursor:
        tmpsource = None
        tmpdestin = None
        files = rows[0]
        artist = rows[1]
        if os.path.isfile(files):
            tmpsource = files
            if depth == 0:
                tmpfile = artist.replace('/', '_') + '-' +(os.path.basename(tmpsource)).replace(' - ', '-')
            else:
                count = 0
                tmpdepth = 0 - depth
                tmpfile = ''
                while count < depth:
                    tmpfile = os.path.join(tmpfile, os.path.dirname(files).split('/')[tmpdepth])
                    tmpdepth = tmpdepth + 1
                    count = count + 1
                tmpfile = os.path.join(tmpfile, (os.path.basename(tmpsource)).replace(' - ', '-'))
            tmpdestin = os.path.join(destination, tmpfile)
            for items in REPLACE:
                tmpdestin = tmpdestin.replace(items, '')
                tmpfile = tmpfile.replace(items, '')
            tmpdestin = tmpdestin.replace('sftphost=', 'sftp:host=')
            #print('\ndestination...\n', destination)
            print('\ncopying...\n', tmpsource)
            # print('\nnot copied\n', files)
            if not os.path.isdir(os.path.dirname(tmpdestin)):
                os.makedirs(os.path.dirname(tmpdestin))
            if not os.path.isfile(tmpdestin):
                #print('\ncopying...\n', tmpdestin)
                shutil.copy2(tmpsource, tmpdestin)
                print('copiedfile\n', tmpdestin)
                destinfiles.append(tmpdestin)
            elif os.path.isfile(tmpdestin):
                destinfiles.append(tmpdestin)
        else:
            print('\nnot copied\n', files)

# cleanup
if cnx and os.path.isdir(destination) and len(destinfiles) != 0:
    for files in os.listdir(destination):
        if os.path.isdir(os.path.join(destination, files)):
            foldersearch(os.path.join(destination, files))
        else:
            filecheck(os.path.join(destination, files))
