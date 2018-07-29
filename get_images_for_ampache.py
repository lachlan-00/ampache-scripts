#!/usr/bin/env python3

""" local album images into mysql

  merge data from your music library into ampache
  -----------------------------------------------

  This script will examine a path for art (currently only 'folder.jpg')

  if it matches the art it will try to insert it into the database
  it will update your database if it doesn't find existing art

"""

import csv
import mimetypes
import os
import shutil
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
        self.csvfile = None
        self.source = None
        self.cnx = None
        self.binarydata = None

        self.settings = 'settings.csv'
        self.artname = 'folder.jpg'
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
            print('found settings file')
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
            current_path = os.listdir(input_string)
            # alphabetically
            # current_path.sort(key=lambda y: y.lower())
            # sort by most recent modification date
            current_path.sort(key=lambda s: os.path.getmtime(os.path.join(input_string, s)), reverse=True)
            for pathfiles in current_path:
                tmppath = os.path.join(input_string, pathfiles)
                if os.path.isdir(tmppath):
                    self.foldersearch(tmppath)
                elif os.path.isfile(tmppath) and pathfiles.lower() == self.artname:
                    # run filesearch
                    #print(tmppath)
                    self.filecheck(tmppath)


    def filecheck(self, input_string):
        self.binarydata = None
        self.binarydata = open(input_string, 'rb').read()
        if self.binarydata:
            self.sqlinserts(input_string.replace(self.artname, ''))

    def sqlinserts(self, source_dir):
        """ check connection then look & insert album art """
        if not self.cnx:
            print('creating database connection')
            try:
                self.cnx = mysql.connector.connect(user=self.dbuser, password=self.dbpass,
                                                   host=self.dbhost, database=self.dbname)
            except mysql.connector.errors.InterfaceError:
                try:
                    self.cnx = mysql.connector.connection.MySQLConnection(user=self.dbuser,
                                                                          password=self.dbpass,
                                                                          host=self.dbhost,
                                                                          database=self.dbname)
                except mysql.connector.errors.InterfaceError:
                    pass
        if self.cnx:
            tmpalbum = self.lookforalbum(source_dir)# + '\t' + str(self.binarydata))
            if tmpalbum:
                self.insertalbum(tmpalbum)
            

    def lookforalbum(self, source_dir):
        """ get the album id from the database matching the filename """
        albumsearch = ('SELECT DISTINCT album FROM song WHERE file LIKE \'' +
                       source_dir.replace("'", "\\'").replace(self.source, '%') + '%\'')
        cursor = self.cnx.cursor(buffered=True)
        try:
            cursor.execute(albumsearch)
            for row in cursor:
                return row[0]
        except mysql.connector.errors.ProgrammingError:
            print('ERROR WITH QUERY:\n' + albumsearch)
            pass
        return None

    def insertalbum(self, album):
        """ check the database for an existing line and insert if missing """
        albumcheck = ('SELECT `object_id` FROM `image` WHERE `object_type` = \'album\' AND `object_id` = ' + str(album) + ';')
        checkcursor = self.cnx.cursor(buffered=True)
        try:
            checkcursor.execute(albumcheck)
            for row in checkcursor:
                if row[0] == album:
                    return False
        except mysql.connector.errors.ProgrammingError:
            print('ERROR WITH QUERY:\n' + albumcheck)
            pass
        albuminsert = ('INSERT INTO `image` (`id`, `image`, `width`, `height`, `mime`, `size`, `object_type`, `object_id`, `kind`) ' +
                       'VALUES (\'0\', %s, \'300\', \'300\', \'image/png\', \'original\', \'album\', '+ str(album) +', \'default\');')
        cursor = self.cnx.cursor(buffered=True)
        try:
            cursor.execute(albuminsert, (self.binarydata, ))
            if cursor.lastrowid != 0:
                print('Inserted ' + album)
        except mysql.connector.errors.ProgrammingError:
            print('ERROR WITH QUERY:\n' + albuminsert)
            pass
        #print(albuminsert)

    def lookforfiles(self, source_dir):
        """ simple file or folder checks """
        if os.path.isdir(source_dir):
           for files in os.listdir(source_dir):
                if os.path.isdir(os.path.join(source_dir, files)):
                    self.foldersearch(os.path.join(source_dir, files))
                else:
                    self.filecheck(os.path.join(source_dir, files))

GETLOCALIMAGES()
