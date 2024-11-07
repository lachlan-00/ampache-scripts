#!/usr/bin/env python3

""" API dump into ampache from tsv

  merge last.fm data with ampache
  -------------------------------

  This script will examine a dump file from lastscrape
  then query your ampache database

  if it matches the artist, album and song
  it will update your database to reflect each play

  Details on the Ampache wiki [here](https://github.com/ampache/ampache/wiki/import-lastfm-data)

"""
import ampache
import csv
import os
import sys
import shutil

process = None
csvfile = None
printallrows = False
printerrors = False
settings = 'ampache.csv'
dumpfile = 'dump.txt'
lovedfile = 'loved.txt'
checkfile = ''

# API format and version
api_format = 'json'
api_version = '6.6.0'
# ampache details
ampache_url = None
ampache_user = None
ampache_api = None

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
    if arguments[:4] == '/all':
        printallrows = True
        printerrors = True
    if arguments[:7] == '/silent':
        printallrows = False
        printerrors = False
    if arguments[:7] == '/errors' or arguments[:6] == '/error':
        printerrors = True
    if arguments[:3] == '/c:':
        process = 'check'
        checkfile = arguments[3:]


# get settings for database
if os.path.isfile('ampache.csv'):
    settings = os.path.join(os.path.dirname(os.path.relpath(__file__)), 'ampache.csv')
if not os.path.isfile(settings):
    settings = os.path.join(os.path.dirname(os.path.relpath(__file__)), settings)
if not os.path.isfile(settings):
    settings = os.path.join(os.path.dirname(os.path.realpath(__file__)), settings)
if os.path.isfile(settings) and not process == 'check':
    print('found settings file')
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
                if row[0] == 'ampache_user':
                    ampache_user = row[1]
                if row[0] == 'ampache_api':
                    ampache_api = row[1]
    csvfile.close()
else:
    # Ampache variables
    ampache_url = 'https://music.com.au'
    ampache_user = 'username'
    ampache_api = 'supersecretapikey'

if os.path.isfile(checkfile) and process == 'check':
    with open(checkfile, 'r') as readfile:
        openfile = csv.reader(readfile, delimiter='\t', )
        if os.path.isfile((checkfile + '.check')):
            shutil.move((checkfile + '.check'), (checkfile + '.old'))
        writefile = open((checkfile + '.check'), 'w')
        for row in openfile:
            try:
                test = row[0]
            except IndexError:
                test = None
            if test:
                tmp0 = row[0]
                tmp1 = row[1]
                tmp2 = row[2]
                tmp3 = row[3]
                tmp4 = ''
                tmp5 = ''
                tmp6 = ''
                try:
                    if not row[4] == '':
                        tmp4 = row[4]
                except IndexError:
                    # missing all the rows in the tsv
                    pass
                try:
                    if not row[5] == '':
                        tmp5 = row[5]
                except IndexError:
                    # missing all the rows in the tsv
                    pass
                try:
                    if not row[6] == '':
                        tmp6 = row[6]
                except IndexError:
                    # missing all the rows in the tsv
                    pass
                tmpline = ('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\n'.format(str(tmp0), str(tmp1),
                                                                        str(tmp2), str(tmp3), str(tmp4),
                                                                        str(tmp5), str(tmp6)))
                writefile.write(tmpline)
    writefile.close()
    readfile.close()


# object_count will store each play for tracks
# with separate rows for artist & album
selectquery = "SELECT * FROM `object_count` ORDER BY `id` DESC"
clearnotplayed = "UPDATE `song` SET played = 0 WHERE played = 1 and id not in (SELECT object_id from object_count)"

notfoundcount = 0
notfoundlist = []

ampache_session = None
if not process == 'check':
    print('Connecting to:\n    ', ampache_url)

    ampacheConnection = ampache.API()
    ampacheConnection.set_debug(False)
    ampacheConnection.set_format(api_format)

    encrypted_key = ampacheConnection.encrypt_string(ampache_api, ampache_user)
    ampache_session = ampacheConnection.handshake(ampache_url, encrypted_key, False, False, api_version)

    if not ampache_session:
        print()
        sys.exit('ERROR: Failed to connect to ' + ampache_url)

if ampache_session:
    print('Connection Established\n')
    if os.path.isfile(dumpfile):
        print('Processing file ' + dumpfile)
        with open(dumpfile, 'r', encoding="utf8") as csvfile:
            # lastscrape is sorted recent -> oldest so reverse that
            # that way the database will have a lower ID for older tracks
            openfile = reversed(list(csv.reader(csvfile, delimiter='\t', )))
            # print tsv header to allow live updates to output file
            if printallrows or printerrors:
                print('date\ttrack\tartist\talbum\ttrackmbid\tartistmbid' +
                      '\talbummbid')
            for row in openfile:
                tmprow = []
                tmpdate = None
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
                    if trackmbid and albummbid and artistmbid:
                        search_rules = [['mbid', 4, trackmbid],['mbid_album', 4, albummbid],['mbid_artist', 4, artistmbid]]

                        # search by id data and return 1 object
                        search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                        songs = ampacheConnection.get_id_list(search_songs, 'song')
                        for song_id in songs:
                            tmpsong = song_id
                    if not rowtrack or not rowartist:
                        pass
                    elif (not tmpsong) and (rowtrack or rowartist or rowalbum):
                        # find missing data if the mbid didn't work
                        if (not tmpsong) and (rowtrack and rowalbum and rowartist):
                            # album artist first
                            search_rules = [['title', 4, rowtrack],['album', 4, rowalbum],['album_artist', 4, rowartist]]

                            # search by id data and return 1 object
                            search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                            songs = ampacheConnection.get_id_list(search_songs, 'song')
                            for song_id in songs:
                                tmpsong = song_id
                            if not tmpsong:
                                #maybe song artist too?
                                search_rules = [['title', 4, rowtrack],['album', 4, rowalbum],['artist', 4, rowartist]]

                                # search by id data and return 1 object
                                search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                                songs = ampacheConnection.get_id_list(search_songs, 'song')
                                for song_id in songs:
                                    tmpsong = song_id
                        # try pairs
                        if (not tmpsong) and (trackmbid and artistmbid):
                            search_rules = [['mbid', 4, trackmbid],['mbid_artist', 4, artistmbid]]

                            # search by id data and return 1 object
                            search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                            songs = ampacheConnection.get_id_list(search_songs, 'song')
                            for song_id in songs:
                                tmpsong = song_id
                        if (not tmpsong) and (rowtrack and rowartist):
                            # album artist first
                            search_rules = [['title', 4, rowtrack],['album_artist', 4, rowartist]]

                            # search by id data and return 1 object
                            search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                            songs = ampacheConnection.get_id_list(search_songs, 'song')
                            for song_id in songs:
                                tmpsong = song_id
                            if not tmpsong:
                                #maybe song artist too?
                                search_rules = [['title', 4, rowtrack],['artist', 4, rowartist]]

                                # search by id data and return 1 object
                                search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                                songs = ampacheConnection.get_id_list(search_songs, 'song')
                                for song_id in songs:
                                    tmpsong = song_id
                        if (not tmpsong) and (trackmbid and albummbid):
                            search_rules = [['mbid', 4, trackmbid],['mbid_album', 4, albummbid]]

                            # search by id data and return 1 object
                            search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                            songs = ampacheConnection.get_id_list(search_songs, 'song')
                            for song_id in songs:
                                tmpsong = song_id
                        if (not tmpsong) and (rowtrack and rowalbum):
                            search_rules = [['title', 4, rowtrack],['album', 4, rowalbum]]

                            # search by id data and return 1 object
                            search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                            songs = ampacheConnection.get_id_list(search_songs, 'song')
                            for song_id in songs:
                                tmpsong = song_id
                        if (not tmpsong) and (not rowalbum) and (rowtrack and rowartist):
                            # album artist first
                            search_rules = [['title', 4, rowtrack],['album_artist', 4, rowartist]]

                            # search by id data and return 1 object
                            search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                            songs = ampacheConnection.get_id_list(search_songs, 'song')
                            for song_id in songs:
                                tmpsong = song_id
                            if not tmpsong:
                                #maybe song artist too?
                                search_rules = [['title', 4, rowtrack],['artist', 4, rowartist]]

                                # search by id data and return 1 object
                                search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                                songs = ampacheConnection.get_id_list(search_songs, 'song')
                                for song_id in songs:
                                    tmpsong = song_id

                    success = False
                    # if you found a song id we have a match
                    if tmpdate and tmpsong:
                        result = ampacheConnection.record_play(tmpsong, False, 'update_ampache_from_file2.py', tmpdate)
                        if printallrows and result and result['success']:
                            success = True

                    if printallrows or ((not tmpdate or not tmpsong) and printerrors):
                        tmp0 = row[0]
                        tmp1 = row[1]
                        tmp2 = row[2]
                        tmp3 = row[3]
                        tmp4 = ''
                        tmp5 = ''
                        tmp6 = ''
                        try:
                            if not row[4] == '':
                                tmp4 = row[4]
                        except IndexError:
                            # missing all the rows in the tsv
                            pass
                        try:
                            if not row[5] == '':
                                tmp5 = row[5]
                        except IndexError:
                            # missing all the rows in the tsv
                            pass
                        try:
                            if not row[6] == '':
                                tmp6 = row[6]
                        except IndexError:
                            # missing all the rows in the tsv
                            pass
                        tmpline = ('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}'.format(str(tmp0), str(tmp1),
                                                                              str(tmp2), str(tmp3), str(tmp4),
                                                                              str(tmp5), str(tmp6)))
                        if success:
                            print('PLAYED', tmpline)
                        else:
                            if not tmpdate or not tmpsong:
                                print('MISSING', tmpline)
                            else:
                                print('FAILED', tmpline)
            # close connections
            csvfile.close()
    if os.path.isfile(lovedfile):
            print('Processing file ' + lovedfile)
            with open(lovedfile, 'r', encoding="utf8") as csvfile:
                # there is no ide in user_flag so we don't need to sort it
                openfile = list(csv.reader(csvfile, delimiter='\t', ))
                # print tsv header to allow live updates to output file
                if printallrows or printerrors:
                    print('date\ttrack\tartist\t[BLANK]\ttrackmbid\tartistmbid')
                for row in openfile:
                    tmprow = []
                    tmpdate = None
                    tmpsong = None
                    founddate = False
                    foundartist = None
                    foundalbum = None
                    foundsong = None
                    rowtrack = None
                    rowartist = None
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
                        # search ampache db for song, album and artist
                        # Look for a musicbrainz ID before the text of the tags
                        # This should hopefully be more reliable if your tags change a lot
                        #
                        # [0]=time [1]=track [2]=artist [3]=trackMBID [4]=artistMBID
                        #
                        if trackmbid and artistmbid:
                            search_rules = [['mbid', 4, trackmbid],['mbid_artist', 4, artistmbid]]

                            # search by id data and return 1 object
                            search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                            songs = ampacheConnection.get_id_list(search_songs, 'song')
                            for song_id in songs:
                                tmpsong = song_id
                        if not rowtrack or not rowartist:
                            pass
                        elif (not tmpsong) and (rowtrack or rowartist):
                            # find missing data if the mbid didn't work
                            if (not tmpsong) and (rowtrack and rowartist):
                                #maybe song artist too?
                                search_rules = [['title', 4, rowtrack],['artist', 4, rowartist]]

                                # search by id data and return 1 object
                                search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                                songs = ampacheConnection.get_id_list(search_songs, 'song')
                                for song_id in songs:
                                    tmpsong = song_id
                            # try pairs
                            if (not tmpsong) and (trackmbid and artistmbid):
                                search_rules = [['mbid', 4, trackmbid],['mbid_artist', 4, artistmbid]]

                                # search by id data and return 1 object
                                search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                                songs = ampacheConnection.get_id_list(search_songs, 'song')
                                for song_id in songs:
                                    tmpsong = song_id
                            if (not tmpsong) and (rowtrack and rowartist):
                                #maybe song artist too?
                                search_rules = [['title', 4, rowtrack],['artist', 4, rowartist]]

                                # search by id data and return 1 object
                                search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                                songs = ampacheConnection.get_id_list(search_songs, 'song')
                                for song_id in songs:
                                    tmpsong = song_id
                            if (not tmpsong) and (rowtrack and rowartist):
                                #maybe song artist too?
                                search_rules = [['title', 4, rowtrack],['artist', 4, rowartist]]

                                # search by id data and return 1 object
                                search_songs = ampacheConnection.advanced_search(search_rules, 'and', 'song', 0, 1)

                                songs = ampacheConnection.get_id_list(search_songs, 'song')
                                for song_id in songs:
                                    tmpsong = song_id

                    success = False
                    # if you found a song id we have a match
                    if tmpdate and tmpsong:
                        result = ampacheConnection.flag('song', tmpsong, True, tmpdate)
                        if printallrows and result and result['success']:
                            success = True

                    if printallrows or ((not tmpdate or not tmpsong) and printerrors):
                        tmp0 = row[0]
                        tmp1 = row[1]
                        tmp2 = row[2]
                        tmp3 = ''
                        tmp4 = ''
                        try:
                            if not row[4] == '':
                                tmp4 = row[4]
                        except IndexError:
                            # missing all the rows in the tsv
                            pass
                        try:
                            if not row[5] == '':
                                tmp5 = row[5]
                        except IndexError:
                            # missing all the rows in the tsv
                            pass
                        tmpline = ('{0}\t{1}\t{2}\t{3}\t{4}'.format(str(tmp0), str(tmp1),
                                                                    str(tmp2), str(tmp3), str(tmp4)))
                        if success:
                            print('LOVED', tmpline)
                        else:
                            if not tmpdate or not tmpsong:
                                print('MISSING', tmpline)
                            else:
                                print('FAILED', tmpline)
            # close connections
            csvfile.close()
