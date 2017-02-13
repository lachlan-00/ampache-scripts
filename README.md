# ampache-scripts
Insert data from Last.fm into your ampache database
GPLv3 <http://www.gnu.org/licenses/>

Currently the project is a single script:
 * mysql-connection.py


NEWS
----

As of 2017-02-13 mysql-connection can be considered stable.

I'm satisfied that only the correct data will be updated and only incorrect or duplicate rows will be removed.

There are multiple checks in place to ensure data is written when all checks are met.

In the current version plays will be skipped if they can't be identified or filtered to an individual song/album/artist.
This is to ensure that all data is correct on instertion to the DB.

In my testing the majority of duplicates are mispelled or are songs without an album that are duplicated at least once in my collection.


ABOUT THE SCRIPT
----------------
I have used Last.fm for a a decade and i want to move away to a private solution.

lastexport.py is used for exporting your listening history (also loved/banned tracks) from last.fm or libre.fm to a text file.
http://bugs.foocorp.net/projects/librefm/wiki/LastToLibre
https://gitorious.org/fmthings/lasttolibre

(the project seems dead now so i'm going to update it to python3 when i have time to do the rest)
https://github.com/lachlan-00/lastscrape-gui/blob/master/lastexport3.py


REQUIREMENTS
------------

python3-mysql.connector


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
