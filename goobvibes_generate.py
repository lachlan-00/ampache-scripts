#!/usr/bin/env python3

import ampache
import configparser
import os
import sys

from xml.dom import minidom

def create_stations_xml():
    # installed from deb/rpm/etc
    directory = os.path.expanduser("~/.local/share/goodvibes")
    if not os.path.exists(directory):
        print("makedir: ", os.path.expanduser("~/.local/share/goodvibes"))
        os.makedirs(directory)
    # flatpak default folder
    flatpakdirectory = os.path.expanduser("~/.var/app/io.gitlab.Goodvibes/data/goodvibes")
    if not os.path.exists(flatpakdirectory):
        print("makedir: ", os.path.expanduser("~/.var/app/io.gitlab.Goodvibes/data/goodvibes"))
        os.makedirs(flatpakdirectory)

    # Create the root element
    root = minidom.Document()
    stations = root.createElement("Stations")
    root.appendChild(stations)
    
    # Connect ot Ampache
    # user variables
    ampache_url = None
    ampache_api = None
    ampache_user = None
    try:
        if sys.argv[1]:
            ampache_url = sys.argv[1]
        if sys.argv[2]:
            ampache_api = sys.argv[2]
        if sys.argv[3]:
            ampache_user = sys.argv[3]
    except IndexError:
        if os.path.isfile('ampyche.conf'):
            conf = configparser.RawConfigParser()
            conf.read('ampyche.conf')
            if not ampache_url:
                ampache_url = conf.get('conf', 'ampache_url')
            if not ampache_api:
                ampache_api = conf.get('conf', 'ampache_apikey')
            if not ampache_user:
                ampache_user = conf.get('conf', 'ampache_user')

    # xml or json supported formats
    api_format = 'json'
    api_version = '6.0.0'

    """
    handshake duplicate_mbid_group
    """
    print('Connecting to:\n    ', ampache_url)
    ampacheConnection = ampache.API()
    ampacheConnection.set_debug(False)
    ampacheConnection.set_format(api_format)
    encrypted_key = ampacheConnection.encrypt_string(ampache_api, ampache_user)
    ampache_session = ampacheConnection.handshake(ampache_url, encrypted_key, False, False, api_version)

    if not ampache_api:
        print()
        sys.exit('ERROR: Failed to connect to ' + ampache_url)

    user = ampacheConnection.user(ampache_user)
    authtoken = user["auth"]
    if "streamtoken" in user:
        authtoken = user["streamtoken"]
    playlists = ampacheConnection.playlists()

    for playlist in sorted(playlists['playlist'], key=lambda x: x["name"]):
        # Create the first station element
        station = root.createElement("Station")
        stations.appendChild(station)
        listtype = "playlist"
        if "smart_" in playlist["id"]:
            listtype = "search"
        uri = root.createElement("uri")
        uri.appendChild(root.createTextNode(ampache_url + "/play/ssid/" + authtoken + "/uid/" + user["id"] + "/random/1/random_type/" + listtype + "/random_id/" + playlist["id"].replace("smart_", "", 1)))
        station.appendChild(uri)
        name = root.createElement("name")
        name.appendChild(root.createTextNode(playlist["name"]))
        station.appendChild(name)


    # Write the XML to file
    with open(os.path.join(directory, "stations.xml"), "w") as f:
        root.writexml(f, indent="  ", newl="\n", addindent="  ", encoding="utf-8")
    # Write the XML to file
    with open(os.path.join(flatpakdirectory, "stations.xml"), "w") as f:
        root.writexml(f, indent="  ", newl="\n", addindent="  ", encoding="utf-8")
    # Write the XML to file
    with open("stations.xml", "w") as f:
        root.writexml(f, indent="  ", newl="\n", addindent="  ", encoding="utf-8")

# Driver Code
if __name__ == "__main__": 
    create_stations_xml()
