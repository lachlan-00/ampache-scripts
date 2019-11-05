#!/usr/bin/env python3

""" get ampache files from mysql

  query ampache database for top rated songs to copy
  --------------------------------------------------

  This script will query your ampache database for top rated songs
  if can find the song by it's path it will copy to your destination

"""

import configparser
import csv
import mimetypes
import os
import requests
import shutil
import sys
import time

import ampache

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
myid = None

listonly = False
playlist_id = 0
output_format = 'mp3'

# destination folder
destination = None
depth = 0
find = None
replace = None
# files that should be in the destination
destinfiles = []

print('\n-------------------------\nget_files_from_ampache.py\n-------------------------')

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
    if arguments[:5].lower() == '/list':
        listonly = True
    if arguments[:3].lower() == '/p:':
        playlist_id = arguments[3:]

# get settings for database
if os.path.isfile('mysettings.csv'):
    SETTINGS = os.path.join(os.path.dirname(os.path.relpath(__file__)), 'mysettings.csv')
if not os.path.isfile(SETTINGS):
    SETTINGS = os.path.join(os.path.dirname(os.path.relpath(__file__)), SETTINGS)
if not os.path.isfile(SETTINGS):
    SETTINGS = os.path.join(os.path.dirname(os.path.realpath(__file__)), SETTINGS)
if os.path.isfile(SETTINGS):
    print('\nLoading local settings file: ' + SETTINGS + '\n')
    with open(SETTINGS, 'r') as csvfile:
        openfile = csv.reader(csvfile)
        for row in openfile:
            try:
                test = row[0]
            except IndexError:
                test = None
            if test:
                if row[0] == 'ampache_url':
                    ampache_url = row[1]
                elif row[0] == 'ampache_user':
                    ampache_user = row[1]
                elif row[0] == 'ampache_api':
                    ampache_api = row[1]
                elif row[0] == 'output_format':
                    output_format = row[1]
                elif row[0] == 'myid':
                    myid = row[1]
                elif row[0] == 'find':
                    find = row[1]
                elif row[0] == 'replace':
                    replace = row[1]
                elif row[0] == 'usbfolder':
                    try:
                        if not destination and os.path.isdir(row[1]):
                            destination = row[1]
                    except NameError:
                        destination = None
                        pass
                    except TypeError:
                        destination = None
                        pass
                elif row[0] == 'usbdepth':
                    depth = int(row[1])
    csvfile.close()

if destination:
    if os.path.isdir(destination):
        print('\nloading... ' + destination)
    # only continue if output is correct
    else:
        destination = None
else:
    sys.exit('Destination not found. use /d: to set an output path\n' +
          '\n   e.g. /media/user/USB/Music\n')

""" ping ampache for auth key """
encrypted_key = ampache.encrypt_string(ampache_api, ampache_user)
ampache_session = ampache.handshake(ampache_url, encrypted_key)

if ampache_session and (listonly or playlist_id == 0):
    playlists = ampache.playlists(ampache_url, ampache_session)
    
    for child in playlists:
        if child.tag == 'playlist':
            #playlist = ampache.playlist(ampache_url, ampache_session, child.attrib['id'])
            print('\nid:    ', child.attrib['id'])
            print('name:  ', child.find('name').text)
            print('items: ', child.find('items').text)
            print('type:  ', child.find('type').text)
            print('owner: ', child.find('owner').text)
            
# process query and copy results to the destination
elif ampache_session and destination and not playlist_id == 0:
    print('Connection Established\n')
    #cursor = cnx.cursor()
    songs = ampache.playlist_songs(ampache_url, ampache_session, playlist_id)
    for child in songs:
        if child.tag == 'total_count':
            continue
        tmpdestin = None
        tmpsource = child.attrib['id']
        file = child.find('filename').text
        title = child.find('title').text
        track = child.find('track').text
        artist = child.find('artist').text
        if tmpsource:
            if depth == 0:
                tmpfile = artist.replace('/', '_') + '-' + (os.path.basename(file)).replace(' - ', '-')
            else:
                count = 0
                tmpdepth = 0 - depth
                tmpfile = ''
                while count < depth:
                    tmpfile = os.path.join(tmpfile, os.path.dirname(file).split('/')[tmpdepth])
                    tmpdepth = tmpdepth + 1
                    count = count + 1
                tmpfile = os.path.join(tmpfile, (os.path.basename(tmpsource)).replace(' - ', '-'))
            # put the correct extension on the file
            tmpfile = os.path.splitext(tmpfile)[0] + '.' + output_format
            tmpdestin = os.path.join(destination, tmpfile)
            for items in REPLACE:
                tmpdestin = tmpdestin.replace(items, '')
                tmpfile = tmpfile.replace(items, '')
            tmpdestin = tmpdestin.replace('sftphost=', 'sftp:host=')
            if not os.path.isdir(os.path.dirname(tmpdestin)):
                print('Making destination', tmpdestin)
                os.makedirs(os.path.dirname(tmpdestin))
            if not os.path.isfile(tmpdestin):
                print('\nIN.....', tmpsource)
                try:
                    #shutil.copy2(tmpsource, tmpdestin)
                    ampache.ping(ampache_url, ampache_session)
                    download = ampache.download(ampache_url, ampache_session, tmpsource, 'song', tmpdestin, output_format)
                    if download != False:
                        print("downloaded", tmpsource, tmpdestin)
                    print('OUT....', tmpdestin)
                    destinfiles.append(tmpdestin)
                except OSError:
                    print('\nFAIL...', files, '\n')
                    pass
            elif os.path.isfile(tmpdestin):
                destinfiles.append(tmpdestin)
        else:
            print('\nFAIL...', files, '\n')

# cleanup
if cnx and os.path.isdir(destination) and len(destinfiles) != 0:
    for files in os.listdir(destination):
        if os.path.isdir(os.path.join(destination, files)):
            foldersearch(os.path.join(destination, files))
        else:
            filecheck(os.path.join(destination, files))

