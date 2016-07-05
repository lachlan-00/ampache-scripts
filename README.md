# ampache-scripts
Insert data from Last.fm into your ampache database

Currently the project is a single script:
 * mysql-connection.py

Right now this is poorly written (just finished today) but it works
I want to expand on this further into an ampache plugin that will update the databse in the same way that last.fm scrobbles work


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
If it can't match the song it will dump each row that was missed at the end so you can try again

USAGE
 * Get a dump for your username.
     python3 ./lastexport3.py -u your_lastfm_username -o dump.txt
 * Run the merge script and get an output dump of bad matches
    python3 ./mysql-connection.py > output.txt

You can also set a specifiv file name using arguments
    python3 ./mysql-connection.py /d:dump.txt > output.txt