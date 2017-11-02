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
import mysql.connector
import urllib.parse
import xml.etree.ElementTree as etree


# Default paths for rhythmbox & the user
HOMEFOLDER = os.getenv('HOME')
PATH = '/.local/share/rhythmbox/'
DB = (HOMEFOLDER + PATH + 'rhythmdb.xml')
# Test # DB = (HOMEFOLDER + PATH + 'rhythmdb-test.xml')
DBBACKUPPATH = (HOMEFOLDER + PATH + 'rhythmdb-backup-merge.xml')


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
        self.rowchanged = 0

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
        try:
            self.cnx = mysql.connector.connect(user=self.dbuser, password=self.dbpass,
                                               host=self.dbhost, database=self.dbname,
                                               connection_timeout=5)
        except mysql.connector.errors.InterfaceError:
            try:
                self.cnx = mysql.connector.connection.MySQLConnection(user=self.dbuser,
                                                                      password=self.dbpass,
                                                                      host=self.dbhost,
                                                                      database=self.dbname,
                                                                      connection_timeout=5)
            except mysql.connector.errors.InterfaceError:
                pass
        #
        # Try to get through with ssh fowarding
        #
        # eg. ssh -L 3306:externalhost:3306 externalhost
        #
        if not self.cnx:
            try:
                self.cnx = mysql.connector.connect(user=self.dbuser, password=self.dbpass,
                                                   host='localhost', database=self.dbname, connection_timeout=5)
            except mysql.connector.errors.InterfaceError:
                pass

    def backuprbdb(self):
        """ only start if the database has been backed up """
        if self.cnx:
            try:
                print('creating rhythmdb backup\n')
                shutil.copy(DB, DBBACKUPPATH)
                self.rbbackup = True
            except FileNotFoundError:
                self.rbbackup = False
            except PermissionError:
                self.rbbackup = False

    def execute(self, query, querytype):
        """ query ampache mysql database """
        if self.cnx and self.rbbackup and querytype == 'play-count':
            print('Connection Established\n')
            self.playcursor = self.cnx.cursor()
            try:
                self.playcursor.execute(query)
            except mysql.connector.errors.ProgrammingError:
                print('ERROR WITH QUERY:\n' + query)
        elif self.cnx and self.rbbackup and querytype == 'rating':
            print('Connection Established\n')
            self.ratingcursor = self.cnx.cursor()
            try:
                self.ratingcursor.execute(query)
            except mysql.connector.errors.ProgrammingError:
                print('ERROR WITH QUERY:\n' + query)
        self.fillrbcache()

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

    def mergeintoamp(self, querytype):
        """ Merge rhythmdb.xml data into mysql """
        self.fillrbcache()
        print('Processing mysql ' + querytype + '\'s using rhythmbox\n')
        changemade = False
        for entry in self.items:
            tmpvalue = None
            tmpsong = None
            tmpartist = None
            tmpalbum = None
            tmppath = None
            self.rowchanged = 0
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
                if info.tag == 'location':
                    tmppath = urllib.parse.unquote(info.text).lower().replace('file://', '').replace("'", "\\'")
            if tmpvalue:
                # Set insert query
                insertquery = ('INSERT INTO rating (`user`, `object_type`, `object_id`, `rating`) ' +
                               'SELECT ' + str(self.myid) + ' AS `user`, \'song\' AS `object_type`, ' +
                               'song.id AS object_id, ' + str(tmpvalue) + ' AS `rating` ' +
                               'FROM song ' +
                               'LEFT JOIN artist on artist.id = song.artist ' +
                               'LEFT JOIN album on album.id = song.album ' +
                               'WHERE ((song.title = \'' + tmpsong.replace("'", "\\'") + '\' AND ' +
                               'artist.name = \'' + tmpartist.replace("'", "\\'") + '\' AND ' +
                               'album.name = \'' + tmpalbum.replace("'", "\\'") + '\') OR (song.file = \'' +
                               tmppath + '\')) AND ' +
                               'song.id NOT IN (SELECT rating.object_id from rating' +
                               ' WHERE rating.object_type = \'song\' and rating.user = ' + str(self.myid) + ');')
                # insert into mysql
                if self.cnx and self.rbbackup:
                    insertcursor = self.cnx.cursor()
                    try:
                        insertcursor.execute(insertquery)
                        if insertcursor.lastrowid != 0 or insertcursor.lastrowid != self.rowchanged:
                            print('Inserted mysql ' + querytype + ' for', tmpsong, 'as', tmpvalue)
                            self.rowchanged = insertcursor.lastrowid
                    except mysql.connector.errors.ProgrammingError:
                        print('ERROR WITH QUERY:\n' + insertquery)


if __name__ == "__main__":
    MERGEAMPBOX()
