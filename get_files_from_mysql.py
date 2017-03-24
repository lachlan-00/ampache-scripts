#!/usr/bin/env python3

""" get ampache files from mysql

  query ampache database for top rated songs to copy
  --------------------------------------------------

  This script will query your ampache database for top rated songs

  if can find the song by it's path it will copy to your destination

"""


import csv
import os
import shutil
import sys
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
           'û', 'ü', 'ý', 'þ', 'ÿ', '¦', ':', '*', '<<')

# Database connector and details
cnx = None
dbuser = None
dbpass = None
dbhost = None
dbname = None

# destination folder
destination = None
# files that should be in the destination
destinfiles = []

# destination need to be an argument in the form of
# /d:$PATH
for arguments in sys.argv:
    if arguments[:3].lower() == '/d:':
        destination = arguments[3:]

# get settings for database
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
    csvfile.close()
else:
    # Database variables
    dbuser = 'username'
    dbpass = 'password'
    dbhost = '127.0.0.1'
    dbname = 'database'

    # Ampache variables
    myid = '2'

# Connect to the mysql database
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
if not os.path.isdir(destination):
    print("data path not found")
# process query and copy results to the destination
elif cnx and os.path.isdir(destination):
    print('Connection Established\n')
    cursor = cnx.cursor()
    tmpquery = ("SELECT song.file FROM song " +
                "WHERE song.id in " +
                "(SELECT object_id FROM `user_flag` " +
                "WHERE object_type = 'song' and user = 2)")
    try:
        cursor.execute(tmpquery)
    except mysql.connector.errors.ProgrammingError:
        print('ERROR WITH QUERY:\n' + tmpquery)
    for rows in cursor:
        tmpsource = None
        tmpdestin = None
        for files in rows:
            if os.path.isfile(files):
                tmpsource = files
                tmpfile = os.path.dirname(files).split('/')[-2]
                # noinspection PyTypeChecker
                tmpfile = tmpfile + '-' + (os.path.basename(tmpsource)).replace(' - ', '-')
                tmpdestin = os.path.join(destination, tmpfile)
                for items in REPLACE:
                    tmpdestin = tmpdestin.replace(items, '')
                if not os.path.isdir(os.path.dirname(tmpdestin)):
                    os.makedirs(os.path.dirname(tmpdestin))
                if not os.path.isfile(tmpdestin):
                    shutil.copy2(tmpsource, tmpdestin)
                    print('\ncopiedfile\n', tmpdestin)
                    destinfiles.append(os.path.basename(tmpdestin))
                elif os.path.isfile(tmpdestin):
                    destinfiles.append(os.path.basename(tmpdestin))
            else:
                print('\nnot copied\n', files)
# cleanup
if cnx and os.path.isdir(destination) and len(destinfiles) != 0:
    for files in os.listdir(destination):
        if files not in destinfiles:
            print(files, ' does not belong here!')
            os.remove(os.path.join(destination, files))
