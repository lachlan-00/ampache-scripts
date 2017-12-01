# ampache-scripts
GPLv3 <http://www.gnu.org/licenses/>

There are three scripts in the project with separate uses:
 * mysql-connection.py
     Insert data from Last.fm into your ampache database

 * get_files_from_mysql.py
     Query ampache database for top rated songs and copy
     to desired path

 * merge-with-rhythmbox.py
     Insert play totals and ratings into rhythmbox using
     Ampache queries as a master source.

REQUIREMENTS
------------

python3-mysql.connector


NEWS
----

Added  directional sync
 * /torb (to pull data into rhythmbox only)
 * /tomysql (to insert ratings that aren't in mysql into the database)

merge-with-rhythmbox is now two-way syncing ratings!

If you set a rating in rhythmbox it will be set on ampache when no rating is found
(Ampache is still the master source so it won't overwrite existing ratings)


ABOUT get_files_from_mysql.py
-----------------------------
This is a basic query and copy script but i found it really goodfor keeping my car USB up to date.

There are a few minor issues with file collisions on the destination but nothing destructive.

WARNING; when you sync the destination folder is cleaned up for tracks that should not be there.
This will delete anything else in the destination so make sure you use an empty folder.

USAGE
-----
Pretty simple, just make sure you have a valid destination and as long as your databse settings are correct it will start copying.

./get_files_from_mysql.py /d:/media/user/USB/music/

After the sync it will clean up old files that shouldn't be there.
This is helpful if you change ratings but will remove any files other than music from the destination.


ABOUT mysql-connection.py
-------------------------
I have used Last.fm for a a decade and i want to move away to a private solution.

lastexport.py is used for exporting your listening history (also loved/banned tracks) from last.fm or libre.fm to a text file.
http://bugs.foocorp.net/projects/librefm/wiki/LastToLibre
https://gitorious.org/fmthings/lasttolibre

(the project seems dead now so i'm going to update it to python3 when i have time to do the rest)
https://github.com/lachlan-00/lastscrape-gui/blob/master/lastexport3.py


USAGE
-----
This script that will parse a text file and add each play to the ampache database.
If it can't match the song it will dump each row that was missed at the end so you can try again.
Once a song has been verified as entered into the database it won't be overwritten each time.

USAGE
 * Get a dump for your username.
     python3 ./lastexport3.py -u your_lastfm_username -o dump.txt
 * Run the merge script and get an output dump of bad matches
    python3 ./mysql-connection.py > output.txt

You can also set a specifiv file name using arguments
    python3 ./mysql-connection.py /d:dump.txt > output.txt

I've added a new argument /all
This will print all rows from the input file


ABOUT merge-with-rhythmbox.py
-----------------------------
Now that last.fm data is merged and you're using ampache as a primary source of truth for playback history you can use it elsewhere.

This script will merge totals and ratings (1-5 stars) back into rhythmbox so you can always be up to date no matter the library in use.

The script will also insert ratings you have made when no rating exists in ampache.

