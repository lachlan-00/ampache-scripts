#!/usr/bin/env python3

""" local album images into mysql

  merge data from your music library into ampache
  -----------------------------------------------

  This script will examine a path for art (currently only 'folder.jpg')

  if it matches the art it will try to insert it into the database
  it will update your database if it doesn't find existing art

"""

import csv
import os
import sys
import time
import mysql.connector


class GETLOCALIMAGES:
    """ merge data from filesystem with ampache """

    def __init__(self):
        self.dbuser = None
        self.dbpass = None
        self.dbhost = None
        self.dbname = None
        self.myid = None
        self.csvfile = None
        self.source = None
        self.cnx = None
        self.binarydata = None
        self.current_dir = None
        self.nowtime = None
        self.lasttime = None

        self.settings = 'settings.csv'
        self.artname = 'folder.jpg'
        self.nowtime = int(time.time())
        self.load()
        self.lookforfiles(self.source)

    def load(self):
        # destination need to be an argument in the form of
        # /d:$PATH
        for arguments in sys.argv:
            if arguments[:3].lower() == '/d:':
                self.source = arguments[3:]
        # fall back to pwd
        if not self.source:
            self.source = os.path.abspath(os.path.curdir)

        # get settings for database
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
            csvfile.close()
        else:
            # Database variables
            self.dbuser = 'username'
            self.dbpass = 'password'
            self.dbhost = '127.0.0.1'
            self.dbname = 'database'

            # Ampache variables
            self.myid = '2'

    def foldersearch(self, input_string):
        """ process dirs or run tag check for files (if mp3) """
        if os.path.isdir(input_string):
            self.current_dir = input_string
            self.currentdir()
            # Sort subfolders alphabetically
            current_path = os.listdir(self.current_dir)
            current_path.sort(key=lambda y: y.lower())
            for pathfiles in current_path:
                tmppath = os.path.join(input_string, pathfiles)
                if os.path.isdir(tmppath):
                    self.foldersearch(tmppath)
                elif os.path.isfile(tmppath) and pathfiles.lower() == self.artname:
                    # run filesearch
                    # print(tmppath)
                    self.filecheck(tmppath)

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
            print('\nCreating Database connection\n')
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

    def filecheck(self, input_string):
        self.binarydata = None
        self.binarydata = open(input_string, 'rb').read()
        if self.binarydata:
            self.sqlinserts(input_string.replace(self.artname, ''))

    def sqlinserts(self, source_dir):
        """ check connection then look & insert album art """
        self.checkdbconn()
        if self.cnx:
            # Look for album ID's where a song matches the path
            self.lookforalbum(source_dir)
        else:
            print('Connection Failed')

    def lookforalbum(self, source_dir):
        """ get the album id from the database matching the filename """
        albumsearch = ('SELECT DISTINCT album FROM song WHERE file LIKE \'' +
                       source_dir.replace("'", "\\'").replace(self.source, '%') + '%\'')
        try:
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(albumsearch)
            for row in cursor:
                # insert albums for multiple disks if present
                self.insertalbum(row[0])
        except mysql.connector.errors.ProgrammingError:
            print('ERROR WITH QUERY:\n' + albumsearch)
            pass
        except BrokenPipeError:
            self.checkdbconn()
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(albumsearch)
            pass
        except ConnectionResetError:
            self.checkdbconn()
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(albumsearch)
            pass
        except mysql.connector.errors.OperationalError:
            self.checkdbconn()
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(albumsearch)
            pass
        return None

    def insertalbum(self, album):
        """ check the database for an existing line and insert if missing """
        albumcheck = ('SELECT `object_id` FROM `image` WHERE `object_type` = \'album\' AND `object_id` = '
                      + str(album) + ';')
        try:
            checkcursor = self.cnx.cursor(buffered=True)
            checkcursor.execute(albumcheck)
            for row in checkcursor:
                if row[0] == album:
                    return False
        except mysql.connector.errors.ProgrammingError:
            print('ERROR WITH QUERY:\n' + albumcheck)
            pass
        except BrokenPipeError:
            self.checkdbconn()
            checkcursor = self.cnx.cursor(buffered=True)
            checkcursor.execute(albumcheck)
            pass
        except ConnectionResetError:
            self.checkdbconn()
            checkcursor = self.cnx.cursor(buffered=True)
            checkcursor.execute(albumcheck)
            pass
        except mysql.connector.errors.OperationalError:
            self.checkdbconn()
            checkcursor = self.cnx.cursor(buffered=True)
            checkcursor.execute(albumcheck)
            pass
        albuminsert = ('INSERT INTO `image` (`id`, `image`, `mime`, `size`, `object_type`, `object_id`, `kind`) ' +
                       'VALUES (\'0\', %s, \'image/png\', \'original\', \'album\', ' + str(album) + ', \'default\');')
        try:
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(albuminsert, (self.binarydata, ))
            if cursor.lastrowid != 0:
                print('Inserted ' + str(album))
        except mysql.connector.errors.ProgrammingError:
            print('ERROR WITH QUERY:\n' + albuminsert)
            pass
        except BrokenPipeError:
            self.checkdbconn()
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(albuminsert, (self.binarydata, ))
            if cursor.lastrowid != 0:
                print('Inserted ' + str(album))
            pass
        except ConnectionResetError:
            self.checkdbconn()
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(albuminsert, (self.binarydata, ))
            if cursor.lastrowid != 0:
                print('Inserted ' + str(album))
            pass
        except mysql.connector.errors.OperationalError:
            self.checkdbconn()
            cursor = self.cnx.cursor(buffered=True)
            cursor.execute(albuminsert, (self.binarydata, ))
            if cursor.lastrowid != 0:
                print('Inserted ' + str(album))
            pass

    def lookforfiles(self, source_dir):
        """ simple file or folder checks """
        if os.path.isdir(source_dir):
            self.current_dir = str(source_dir)
            self.currentdir()
            # Search initial library path
            current_path = os.listdir(self.current_dir)
            # alphabetically
            current_path.sort(key=lambda y: y.lower())
            # or by most recent modification date
            # current_path.sort(key=lambda s: os.path.getmtime(os.path.join(self.current_dir, s)), reverse=True)
            print('Searching ' + self.source)
            for files in current_path:
                if os.path.isdir(os.path.join(source_dir, files)):
                    self.foldersearch(os.path.join(source_dir, files))
                else:
                    self.filecheck(os.path.join(source_dir, files))

    def currentdir(self):
        """ print the current directory being searched after a time limit """
        self.nowtime = int(time.time())
        if not self.lasttime:
            self.lasttime = int(time.time())
        if int(self.nowtime - self.lasttime) > 10:
            self.lasttime = self.nowtime
            self.nowtime = int(time.time())
            print('Searching ' + self.current_dir.replace(self.source, ''))


GETLOCALIMAGES()
