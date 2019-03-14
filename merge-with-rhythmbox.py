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
import time
import mysql.connector
import urllib.parse
import xml.etree.ElementTree as ElementTree


# Default paths for rhythmbox & the user
HOMEFOLDER = os.getenv('HOME')
PATH = '/.local/share/rhythmbox/'
DB = (HOMEFOLDER + PATH + 'rhythmdb.xml')
DBBACKUPPATH = (HOMEFOLDER + PATH + 'rhythmdb-backup-merge.xml')

# By default there is a two way sync between rhythmbox and MYSQL
# using MYSQL as the primary source.
#
# First MYSQL -> rhythmbox (which will overwrite conflicting rhythmbox data)
# Then rhythmbox -> MYSQL looking for missing information
#
# Skip sections by using the following arguments
#
# /toRB (Sync MYSQL -> rhythmbox)
# /toMYSQL (Sync rhythmbox -> MYSQL)
# -h, --h (Show help)
TORB = False
TOMYSQL = False
HELP = False
HELPMSG = ('\n# By default there is a two way sync between rhythmbox and MYSQL\n' +
           '# using MYSQL as the primary source.\n\n# First MYSQL -> rhythmbox' +
           '(which will overwrite conflicting rhythmbox data)\n' +
           '# Then rhythmbox -> MYSQL looking for missing information\n\n' +
           '# Skip sections by using the following arguments\n ' +
           '  /toRB (Sync MYSQL -> rhythmbox)\n   /toMYSQL (Sync rhythmbox -> MYSQL)\n')

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
        if os.path.isfile('settings.csv'):
            self.settings = 'settings.csv'
        elif os.path.isfile(os.path.join(os.path.dirname(os.path.relpath(__file__)), 'settings.csv')):
            self.settings = os.path.join(os.path.dirname(os.path.relpath(__file__)), 'settings.csv')
        elif os.path.isfile(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.csv')):
            self.settings = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.csv')
        else:
            self.settings = None

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

        # Run if settings file is found
        if self.settings:
            self.run()
        else:
            print('\nError: Unable to load settings file.\n')

    def run(self):
        if TORB and TOMYSQL:
            print('\nDon\'t use /toRB and /toMYSQL together')
            return None
        elif HELP:
            print(HELPMSG)
            return None
        """ Run program """
        self.findsettings()
        self.checkdbconn()
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
        self.playcursor = self.cnx.cursor(buffered=True)
        self.ratingcursor = self.cnx.cursor(buffered=True)
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
        if not os.path.isfile(self.settings):
            self.settings = os.path.join(os.path.dirname(os.path.relpath(__file__)), self.settings)
        if not os.path.isfile(self.settings):
            self.settings = os.path.join(os.path.dirname(os.path.realpath(__file__)), self.settings)
        if os.path.isfile(self.settings):
            print('Found settings file')
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

    def checkdbconn(self):
        """ Maintain database connection """
        if self.cnx:
            # Check existing connection
            if self.cnx.is_connected():
                return
            if not self.cnx.is_connected():
                print('\nError: Reconnecting to database\n')
                self.cnx.reconnect(attempts=4, delay=4)
                return
        if not self.cnx:
            time.sleep(5)
            # Create a new DB connection
            print('\nCreating Database connection')
            try:
                self.cnx = mysql.connector.connect(user=self.dbuser, password=self.dbpass,
                                                   host=self.dbhost, database=self.dbname, connection_timeout=5)
                print('Connected')
            except mysql.connector.errors.InterfaceError:
                try:
                    self.cnx = mysql.connector.connection.MySQLConnection(user=self.dbuser,
                                                                          password=self.dbpass,
                                                                          host=self.dbhost,
                                                                          database=self.dbname, connection_timeout=5)
                    print('Connected')
                except mysql.connector.errors.InterfaceError:
                    pass
        #
        # Try to get through with ssh fowarding
        #
        # eg. ssh -L 3306:localhost:3306 externalhost
        #
        if not self.cnx:
            print("Trying localhost DB connections")
            try:
                self.cnx = mysql.connector.connect(user=self.dbuser, password=self.dbpass,
                                                   host='127.0.0.1', database=self.dbname, connection_timeout=5)
                print('Connected')
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
        """ query ampache MySQL database """
        if self.cnx and self.rbbackup and querytype == 'play-count':
            try:
                self.playcursor = self.cnx.cursor(buffered=True)
                self.playcursor.execute(query)
            except mysql.connector.errors.ProgrammingError:
                print('ERROR WITH QUERY:\n' + query)
                pass
            except BrokenPipeError:
                self.checkdbconn()
                self.playcursor = self.cnx.cursor(buffered=True)
                self.playcursor.execute(query)
                pass
            except ConnectionResetError:
                self.checkdbconn()
                self.playcursor = self.cnx.cursor(buffered=True)
                self.playcursor.execute(query)
                pass
            except mysql.connector.errors.OperationalError:
                self.checkdbconn()
                self.playcursor = self.cnx.cursor(buffered=True)
                self.playcursor.execute(query)
                pass
        elif self.cnx and self.rbbackup and querytype == 'rating':
            try:
                self.ratingcursor = self.cnx.cursor(buffered=True)
                self.ratingcursor.execute(query)
            except mysql.connector.errors.ProgrammingError:
                print('ERROR WITH QUERY:\n' + query)
                pass
            except BrokenPipeError:
                self.checkdbconn()
                self.ratingcursor = self.cnx.cursor(buffered=True)
                self.ratingcursor.execute(query)
                pass
            except ConnectionResetError:
                self.checkdbconn()
                self.ratingcursor = self.cnx.cursor(buffered=True)
                self.ratingcursor.execute(query)
                pass
            except mysql.connector.errors.OperationalError:
                self.checkdbconn()
                self.ratingcursor = self.cnx.cursor(buffered=True)
                self.ratingcursor.execute(query)
                pass
        if self.cnx.is_connected():
            self.fillrbcache()
        else:
            return False

    def fillrbcache(self):
        """ only process id db found and backup created """
        if os.path.isfile(DB) and self.rbbackup:
            # search for plays by artist, track AND album
            # open the database
            print('Opening rhythmdb...\n')
            self.root = ElementTree.parse(os.path.expanduser(DB)).getroot()
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
        """ Merge MySQL data into rhythmdb.xml """
        try:
            if query:
                print('Processing rhythmdb ' + querytype + '\'s using MySQL\n')
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
                                    print('Update: ' + querytype + ' for', row[0], 'from ' + tmpplay + ' to', row[6])
                                    info.text = str(row[6])
                                    mergeplays = True
                        if not mergeplays:
                            changemade = True
                            print('Insert: ' + querytype + ' for', row[0], 'as', row[6])
                            insertplaycount = ElementTree.SubElement(entry, querytype)
                            insertplaycount.text = str(row[6])
                if changemade:
                    print(querytype + 's from MySQL have been inserted into the database.')
                    # Save changes
                    print('saving changes...\n')
                    output = ElementTree.ElementTree(self.root)
                    output.write(os.path.expanduser(DB), encoding="utf-8")
                else:
                    print('No ' + querytype + ' changed')
            else:
                print('no ' + querytype + ' data found\n')
        except ReferenceError:
            # connection lost again!
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
        """ Merge rhythmdb.xml data into MySQL """
        self.fillrbcache()
        print('Processing MySQL ' + querytype + '\'s using rhythmbox\n')
        for entry in self.items:
            tmpvalue = None
            tmpsong = None
            tmppath = None
            rowchanged = 0
            for info in entry:
                if info.tag == querytype:
                    # found a rating. I have seen wildly incorrect numbers in my xml for some reason
                    if str(info.text) in ('1', '2', '3', '4', '5'):
                        tmpvalue = str(info.text)
                if info.tag == 'title':
                    tmpsong = str(info.text)
                if info.tag == 'location':
                    tmppath = urllib.parse.unquote(info.text).lower().replace('file://', '').replace("'", "\\'")
            if tmpvalue:
                # Set insert query
                insertpathquery = ('INSERT INTO rating (`user`, `object_type`, `object_id`, `rating`) ' +
                                   'SELECT ' + str(self.myid) + ' AS `user`, \'song\' AS `object_type`, ' +
                                   'song.id AS object_id, ' + str(tmpvalue) + ' AS `rating` ' +
                                   'FROM song ' +
                                   'LEFT JOIN artist on artist.id = song.artist ' +
                                   'LEFT JOIN album on album.id = song.album ' +
                                   'WHERE LOWER(song.file) = \'' + tmppath.lower().replace(self.replace, self.find) +
                                   '\' AND song.id NOT IN (SELECT rating.object_id from rating' +
                                   ' WHERE rating.object_type = \'song\' and rating.user = ' + str(self.myid) + ');')
                # insert into mysql
                if self.cnx and self.rbbackup:
                    try:
                        insertcursor = self.cnx.cursor(buffered=True)
                        insertcursor.execute(insertpathquery)
                        if insertcursor.lastrowid != 0 or insertcursor.lastrowid != rowchanged:
                            print('MySQL Insert: ' + querytype + ' for', tmpsong, 'as', tmpvalue)
                            rowchanged = insertcursor.lastrowid
                    except mysql.connector.errors.ProgrammingError:
                        print('ERROR WITH QUERY:\n' + insertpathquery)
                        insertcursor = self.cnx.cursor(buffered=True)
                        insertcursor.execute(insertpathquery)
                        if insertcursor.lastrowid != 0 or insertcursor.lastrowid != rowchanged:
                            print('MySQL Insert: ' + querytype + ' for', tmpsong, 'as', tmpvalue)
                            rowchanged = insertcursor.lastrowid
                        pass
                    except BrokenPipeError:
                        self.checkdbconn()
                        insertcursor = self.cnx.cursor(buffered=True)
                        insertcursor.execute(insertpathquery)
                        if insertcursor.lastrowid != 0 or insertcursor.lastrowid != rowchanged:
                            print('MySQL Insert: ' + querytype + ' for', tmpsong, 'as', tmpvalue)
                            rowchanged = insertcursor.lastrowid
                        pass
                    except ConnectionResetError:
                        self.checkdbconn()
                        insertcursor = self.cnx.cursor(buffered=True)
                        insertcursor.execute(insertpathquery)
                        if insertcursor.lastrowid != 0 or insertcursor.lastrowid != rowchanged:
                            print('MySQL Insert: ' + querytype + ' for', tmpsong, 'as', tmpvalue)
                            rowchanged = insertcursor.lastrowid
                        pass
                    except mysql.connector.errors.OperationalError:
                        self.checkdbconn()
                        insertcursor = self.cnx.cursor(buffered=True)
                        insertcursor.execute(insertpathquery)
                        if insertcursor.lastrowid != 0 or insertcursor.lastrowid != rowchanged:
                            print('MySQL Insert: ' + querytype + ' for', tmpsong, 'as', tmpvalue)
                            rowchanged = insertcursor.lastrowid
                        pass


if __name__ == "__main__":
    MERGEAMPBOX()
