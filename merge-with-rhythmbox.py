#!/usr/bin/env python3

""" query ampache mysql and merge into rhythmbox db

  merge data from ampache with rhythmbox
  --------------------------------------

  This script will examine a dump file from lastscrape
  then query your ampache database

  if it matches the artist, album and song
  it will update your databse to reflect each play

"""

import csv
import os
import shutil
import sys
import mysql.connector
import urllib.parse
import xml.etree.ElementTree as etree


# Default paths for rhythmbox & the user
HOMEFOLDER = os.getenv('HOME')
PATH = '/.local/share/rhythmbox/'
DB = (HOMEFOLDER + PATH + 'rhythmdb.xml')
# Test # DB = (HOMEFOLDER + PATH + 'rhythmdb-test.xml')
DBBACKUPPATH = (HOMEFOLDER + PATH + 'rhythmdb-backup-merge.xml')

# Skip sections by using the following arguments
#
# /toRB (Only sync to rhythmbox)
# /toMYSQL (Send data to MYSQL only)
# -h, --h (Show help)
TORB = False
TOMYSQL = False
HELP = False
HELPMSG = ('\n# Skip sections by using the following arguments\n ' +
           '  /toRB (Only sync to rhythmbox)\n   /toMYSQL (Send data to MYSQL only)\n')

for arguments in sys.argv:
    if arguments[:5].lower() == '/torb':
        TORB = True
    if arguments[:8].lower() == '/tomysql':
        TOMYSQL = True
    if arguments[:3].lower() == '--h' or arguments[:2].lower() == '-h':
        HELP = True


def main():
    MERGEAMPBOX()


class MERGEAMPBOX:
    """ merge data from ampache with rhythmbox """

    def __init__(self):
        """ query ampache mysql and merge into rhythmbox db """
        # Connection and queries
        self.cnx = None
        self.playcursor = None
        self.ratingcursor = None
        self.noratingcursor = None
        self.root = None
        self.items = None
        self.playquery = None
        self.ratingquery = None
        self.noratingquery = None

        # Default file names
        self.settings = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.csv')

        # Variable data
        self.dbuser = None
        self.dbpass = None
        self.dbhost = None
        self.dbname = None
        self.myid = None
        self.rbbackup = False
        self.find = None
        self.replace = None
        self.rbcache = []
        self.rbfilecache = []

        self.run()

    def run(self):
        if TORB and TOMYSQL:
            print('\nDon\'t use /toRB and /toMYSQL together')
            return None
        elif HELP:
            print(HELPMSG)
            return None
        """ Run program """
        self.findsettings()
        self.connectdb()
        self.backuprbdb()

        # Total playcount for songs in ampache db
        self.playquery = ('SELECT DISTINCT song.title, artist.name, album.name, ' +
                          'CASE WHEN song.mbid IS NULL THEN \'\' ELSE song.mbid END as smbid, ' +
                          'CASE WHEN artist.mbid IS NULL THEN \'\' ELSE artist.mbid END as ambid, ' +
                          'CASE WHEN album.mbid IS NULL THEN \'\' ELSE album.mbid END as almbid, ' +
                          'COUNT(object_count.object_id), ' +
                          'song.file ' +
                          'FROM object_count ' +
                          'INNER JOIN song on song.id = object_count.object_id ' +
                          ' AND object_count.object_type = \'song\' ' +
                          'LEFT JOIN artist on artist.id = song.artist ' +
                          'LEFT JOIN album on album.id = song.album ' +
                          'WHERE object_count.object_type = \'song\' ' +
                          'GROUP BY song.title, artist.name, album.name, smbid, ambid, almbid;')

        if not self.cnx:
            print('\nNO CONNECTION TO MYSQL\n')
            return False
        # current rating for songs in ampache db
        self.ratingquery = ('SELECT DISTINCT song.title, artist.name, album.name, ' +
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
                            'rating.user = ' + str(self.myid))
        # Run only certain queries depending on arguments
        if TORB and not TOMYSQL:
            print('Importing into Rhythmbox ONLY\n')
            # Run query and cache rhythmbox database
            self.execute(self.playquery, 'play-count')
            # search and update rhythmbox database
            self.mergeintorb(self.playcursor, 'play-count')

            # Run query and cache rhythmbox database
            self.execute(self.ratingquery, 'rating')
            # search and update rhythmbox database
            self.mergeintorb(self.ratingcursor, 'rating')

        elif TOMYSQL and not TORB:
            print('Importing from Rhythmbox into MYSQL ONLY\n')
            # search and update MYSQL using rhythmbox data
            self.mergeintoamp('rating')

        else:
            # Run query and cache rhythmbox database
            self.execute(self.playquery, 'play-count')
            # search and update rhythmbox database
            self.mergeintorb(self.playcursor, 'play-count')

            # Run query and cache rhythmbox database
            self.execute(self.ratingquery, 'rating')
            # search and update rhythmbox database
            self.mergeintorb(self.ratingcursor, 'rating')

            # search and update MYSQL using rhythmbox data
            self.mergeintoamp('rating')

    def findsettings(self):
        """ get settings for database """
        if os.path.isfile(self.settings):
            print('found settings file\n')
            with open(self.settings, 'r') as csvfile:
                openfile = csv.reader(csvfile)
                for row in openfile:
                    try:
                        test = row[0]
                    except IndexError:
                        test = None
                    if test:
                        if row[0] == 'dbuser':
                            self.dbuser = row[1]
                        elif row[0] == 'dbpass':
                            self.dbpass = row[1]
                        elif row[0] == 'dbhost':
                            self.dbhost = row[1]
                        elif row[0] == 'dbname':
                            self.dbname = row[1]
                        elif row[0] == 'myid':
                            self.myid = row[1]
                        elif row[0] == 'find':
                            self.find = row[1]
                        elif row[0] == 'replace':
                            self.replace = row[1]
            csvfile.close()

    def connectdb(self):
        """ Connect to MYSQL database """
        print("Connect to MYSQL...")
        try:
            self.cnx = mysql.connector.connect(user=self.dbuser, password=self.dbpass,
                                               host=self.dbhost, database=self.dbname,
                                               connection_timeout=10)
        except mysql.connector.errors.InterfaceError:
            try:
                self.cnx = mysql.connector.connection.MySQLConnection(user=self.dbuser,
                                                                      password=self.dbpass,
                                                                      host=self.dbhost,
                                                                      database=self.dbname,
                                                                      connection_timeout=10)
            except mysql.connector.errors.InterfaceError:
                pass
        #
        # Try to get through with ssh fowarding
        #
        # eg. ssh -L 3306:externalhost:3306 externalhost
        #
        if not self.cnx:
            print("trying localhost DB connections")
            try:
                self.cnx = mysql.connector.connect(user=self.dbuser, password=self.dbpass,
                                                   host='127.0.0.1', database=self.dbname, connection_timeout=10)
            except mysql.connector.errors.InterfaceError:
                pass

    def backuprbdb(self):
        """ only start if the local database has been backed up """
        if self.cnx:
            try:
                print('creating rhythmdb backup\n')
                shutil.copy(DB, DBBACKUPPATH)
                self.rbbackup = True
            except FileNotFoundError:
                self.rbbackup = False
            except PermissionError:
                self.rbbackup = False
        return

    def execute(self, query, querytype):
        """ query ampache mysql database """
        if self.cnx and self.rbbackup and querytype == 'play-count':
            cnxset = False
            cnxcount = 0
            while not cnxset and cnxcount < 3:
                cnxcount = cnxcount + 1
                try:
                    self.playcursor = self.cnx.cursor()
                    self.playcursor.execute(query)
                    print('Connection Established\n')
                    cnxset = True
                except mysql.connector.errors.ProgrammingError:
                    print('ERROR WITH QUERY:\n' + query)
                except mysql.connector.errors.OperationalError:
                    print('Connection lost... retrying')
                    self.connectdb()
        elif self.cnx and self.rbbackup and querytype == 'rating':
            cnxset = False
            cnxcount = 0
            while not cnxset and cnxcount < 4:
                cnxcount = cnxcount + 1
                try:
                    self.ratingcursor = self.cnx.cursor()
                    self.ratingcursor.execute(query)
                    print('Connection Established\n')
                    cnxset = True
                except mysql.connector.errors.ProgrammingError:
                    print('ERROR WITH QUERY:\n' + query)
                except mysql.connector.errors.OperationalError:
                    print('Connection lost... retrying')
                    self.connectdb()
        if cnxset:
            self.fillrbcache()
        else:
            return False

    def fillrbcache(self):
        """ only process id db found and backup created """
        if os.path.isfile(DB) and self.rbbackup:
            # search for plays by artist, track AND album
            # open the database
            print('Opening rhythmdb...\n')
            self.root = etree.parse(os.path.expanduser(DB)).getroot()
            self.items = [s for s in self.root.getiterator("entry")
                          if s.attrib.get('type') == 'song']
            if self.items and self.cnx:
                self.rbcache = []
                self.rbfilecache = []
                print('Building song data for caches...\n')
                for entries in self.items:
                    data = {}
                    filedata = {}
                    if entries.attrib.get('type') == 'song':
                        for info in entries:
                            if info.tag in ('title', 'artist', 'album', 'mb-trackid', 'mb-artistid', 'mb-albumid'):
                                data[info.tag] = urllib.parse.unquote(info.text.lower())
                            if info.tag in 'location':
                                filedata[info.tag] = urllib.parse.unquote(info.text).lower().replace('file://', '')
                    try:
                        self.rbcache.append('%(title)s\t%(artist)s\t%(album)s\t%(mb-trackid)s' +
                                            '\t%(mb-artistid)s\t%(mb-albumid)s' % data)
                    except KeyError:
                        self.rbcache.append('%(title)s\t%(artist)s\t%(album)s\t\t\t' % data)
                    self.rbfilecache.append('%(location)s' % filedata)

    def mergeintorb(self, query, querytype):
        """ Merge mysql data into rhythmdb.xml """
        try:
            if query:
                print('Processing rhythmdb ' + querytype + '\'s using mysql\n')
                changemade = False
                for row in query:
                    mergeplays = False
                    idx = None
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
                            if tmpcheck in self.rbcache:
                                idx = self.rbcache.index(tmpcheck)
                        if not idx:
                            # When you can't match tags, check filename
                            if self.find and self.replace:
                                tmpfilecheck = str(row[7].lower()).replace(self.find, self.replace)
                            else:
                                tmpfilecheck = str(row[7].lower())
                            if tmpfilecheck in self.rbfilecache:
                                idx = self.rbfilecache.index(tmpfilecheck)
                    # if the index is found, update the database
                    if idx:
                        entry = self.items[idx]
                        for info in entry:
                            if info.tag == querytype:
                                tmpplay = str(info.text)
                                if str(info.text) == str(row[6]):
                                    mergeplays = True
                                elif not str(info.text) == str(row[6]):
                                    changemade = True
                                    print('Updating ' + querytype + ' for', row[0], 'from ' + tmpplay + ' to', row[6])
                                    info.text = str(row[6])
                                    mergeplays = True
                        if not mergeplays:
                            changemade = True
                            print('Inserting ' + querytype + ' for', row[0], 'as', row[6])
                            insertplaycount = etree.SubElement(entry, querytype)
                            insertplaycount.text = str(row[6])
                if changemade:
                    print(querytype + 's from mysql have been inserted into the database.\n')
                    # Save changes
                    print('saving changes')
                    output = etree.ElementTree(self.root)
                    output.write(os.path.expanduser(DB), encoding="utf-8")
                else:
                    print('No ' + querytype + ' changed')
            else:
                print('no ' + querytype + ' data found\n')
        except ReferenceError:
            #connection lost again!
            if querytype == 'play-count':
                # Run query and cache rhythmbox database
                self.execute(self.playquery, 'play-count')
                # search and update rhythmbox database
                self.mergeintorb(self.playcursor, 'play-count')
            if querytype == 'rating':
                # Run query and cache rhythmbox database
                self.execute(self.ratingquery, 'rating')
                # search and update rhythmbox database
                self.mergeintorb(self.ratingcursor, 'rating')


    def mergeintoamp(self, querytype):
        """ Merge rhythmdb.xml data into mysql """
        self.fillrbcache()
        print('Processing mysql ' + querytype + '\'s using rhythmbox\n')
        for entry in self.items:
            tmpvalue = None
            tmpsong = None
            tmpartist = None
            tmpalbum = None
            tmptrack = None
            tmpdisc = None
            tmppath = None
            rowchanged = 0
            for info in entry:
                if info.tag == querytype:
                    # found a rating. I have seen wildly incorrect numbers in my xml for some reason
                    if str(info.text) in ('1', '2', '3', '4', '5'):
                        tmpvalue = str(info.text)
                if info.tag == 'title':
                    tmpsong = str(info.text)
                if info.tag == 'artist':
                    tmpartist = str(info.text)
                if info.tag == 'album':
                    tmpalbum = str(info.text)
                if info.tag == 'track-number':
                    tmptrack = str(info.text)
                if info.tag == 'disc-number':
                    tmpdisc = str(info.text)
                if info.tag == 'location':
                    tmppath = urllib.parse.unquote(info.text).lower().replace('file://', '').replace("'", "\\'")
            if tmpdisc == None:
                tmpdisc = '0'
            if tmpvalue:
                # Set insert query
                insertpathquery = ('INSERT INTO rating (`user`, `object_type`, `object_id`, `rating`) ' +
                                   'SELECT ' + str(self.myid) + ' AS `user`, \'song\' AS `object_type`, ' +
                                   'song.id AS object_id, ' + str(tmpvalue) + ' AS `rating` ' +
                                   'FROM song ' +
                                   'LEFT JOIN artist on artist.id = song.artist ' +
                                   'LEFT JOIN album on album.id = song.album ' +
                                   'WHERE LOWER(song.file) = \'' + tmppath.lower().replace(self.replace, self.find) + '\' AND ' +
                                   'song.id NOT IN (SELECT rating.object_id from rating' +
                                   ' WHERE rating.object_type = \'song\' and rating.user = ' + str(self.myid) + ');')
                #insertquery = ('INSERT INTO rating (`user`, `object_type`, `object_id`, `rating`) ' +
                #               'SELECT ' + str(self.myid) + ' AS `user`, \'song\' AS `object_type`, ' +
                #               'song.id AS object_id, ' + str(tmpvalue) + ' AS `rating` ' +
                #               'FROM song ' +
                #               'LEFT JOIN artist on artist.id = song.artist ' +
                #               'LEFT JOIN album on album.id = song.album ' +
                #               'WHERE (song.title = \'' + tmpsong.replace("'", "\\'") + '\' AND ' +
                #               'artist.name = \'' + tmpartist.replace("'", "\\'") + '\' AND ' +
                #               'album.name = \'' + tmpalbum.replace("'", "\\'") + '\' AND ' +
                #               'song.track = \'' + tmptrack + '\' AND ' +
                #               'album.disk = \'' + tmpdisc + '\') AND song.id NOT IN (SELECT rating.object_id from rating' +
                #               ' WHERE rating.object_type = \'song\' and rating.user = ' + str(self.myid) + ');')
                # insert into mysql
                if self.cnx and self.rbbackup:
                    insertcursor = self.cnx.cursor()
                    try:
                        insertcursor.execute(insertpathquery)
                        #print(insertpathquery)
                        if insertcursor.lastrowid != 0 or insertcursor.lastrowid != rowchanged:
                            print('Inserted mysql ' + querytype + ' for', tmpsong, 'as', tmpvalue)
                            rowchanged = insertcursor.lastrowid
                        #else:
                        #    insertcursor.execute(insertquery)
                        #    if insertcursor.lastrowid != 0 or insertcursor.lastrowid != rowchanged:
                        #        print('Inserted mysql ' + querytype + ' for', tmpsong, 'as', tmpvalue)
                        #        rowchanged = insertcursor.lastrowid
                    except mysql.connector.errors.ProgrammingError:
                        print('ERROR WITH QUERY:\n' + insertquery)


if __name__ == "__main__":
    MERGEAMPBOX()
