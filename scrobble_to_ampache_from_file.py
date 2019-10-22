#!/usr/bin/env python3

""" mysql dump into ampache from tsv

  merge last.fm data with ampache
  -------------------------------

  This script will examine a dump file from lastscrape
  then query your ampache database

  if it matches the artist, album and song
  it will update your database to reflect each play

"""

import configparser
import csv
import os
import time
import threading
import sys

import ampache

csvfile = None
settings = 'settings.csv'
dumpfile = 'dump.txt'

# get dump file name from arguments
for arguments in sys.argv:
    if arguments[:3] == '/d:':
        dumpfile = arguments[3:]

# get settings for database
if os.path.isfile('mysettings.csv'):
    settings = os.path.join(os.path.dirname(os.path.relpath(__file__)), 'mysettings.csv')
if not os.path.isfile(settings):
    settings = os.path.join(os.path.dirname(os.path.relpath(__file__)), settings)
if not os.path.isfile(settings):
    settings = os.path.join(os.path.dirname(os.path.realpath(__file__)), settings)
if os.path.isfile(settings):
    print('found settings file', settings)
    with open(settings, 'r') as csvfile:
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
    csvfile.close()

""" ping ampache for auth key """
encrypted_key = ampache.encrypt_string(ampache_api, ampache_user)
ampache_session = ampache.handshake(ampache_url, encrypted_key)

if ampache_session:
    print('Connection Established\n')
    executionlist = []
    if os.path.isfile(dumpfile):
        print('Processing file ' + dumpfile)
        with open(dumpfile, 'r') as csvfile:
            # lastscrape is sorted recent -> oldest so reverse that
            # that way the database will have a lower ID for older tracks
            openfile = list(csv.reader(csvfile, delimiter='\t', ))
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
                try:
                    test2 = str(int(row[0]))
                except ValueError:
                    test2 = None
                    # print(row)
                except IndexError:
                    test2 = None
                    # print(row)
                if test and test2:
                    # Normalise row data
                    tmpdate = test2
                    try:
                        if not row[1] == '':
                            rowtrack = row[1]
                        if not row[2] == '':
                            rowartist = row[2]
                        if not row[3] == '':
                            rowalbum = row[3]
                    except IndexError:
                        # missing rows in the tsv
                        pass
                    try:
                        if not row[4] == '':
                            trackmbid = row[4]
                    except IndexError:
                        # missing all the rows in the tsv
                        pass
                    try:
                        if not row[5] == '':
                            artistmbid = row[5]
                    except IndexError:
                        # missing all the rows in the tsv
                        pass
                    try:
                        if not row[6] == '':
                            albummbid = row[6]
                    except IndexError:
                        # missing all the rows in the tsv
                        pass

                    # search ampache db for song, album and artist
                    # Look for a musicbrainz ID before the text of the tags
                    # This should hopefully be more reliable if your tags change a lot
                    #
                    # [0]=time [1]=track [2]=artist [3]=album [4]=trackMBID [5]=artistMBID [6]=albumMBID
                    #
                    if not rowtrack == None and not rowartist == None and not rowalbum == None:
                        ampache.ping(ampache_url, ampache_session)
                        scrobble = ampache.scrobble(ampache_url, ampache_session, str(rowtrack), str(rowartist), str(rowalbum),
                                      str(trackmbid).replace("None", ""), str(artistmbid).replace("None", ""), str(albummbid).replace("None", ""),
                                      str(row[0]))
                        if scrobble != False:
                            print(str(row[0]), str(rowtrack), str(rowartist), str(rowalbum),
                              str(trackmbid).replace("None", ""), str(artistmbid).replace("None", ""), str(albummbid).replace("None", ""))
                

