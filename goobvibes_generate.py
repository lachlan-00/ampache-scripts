#!/usr/bin/env python3

import ampache
import os
import sys
from xml.dom import minidom

def ampache_loader(ampacheConnection: ampache.API):
    # Connect to Ampache
    # user variables
    ampache_url = None
    ampache_api = None
    ampache_user = None

    # load a saved config file
    config_path = os.path.expanduser("~/.config/ampache")
    if not os.path.exists(config_path):
        print("makedir: ", config_path)
        os.makedirs(config_path)
    try:
        ampacheConnection.set_config_path(config_path)
        if ampacheConnection.get_config():
            print("get_config:", os.path.join(ampacheConnection.CONFIG_PATH, ampacheConnection.CONFIG_FILE))
            return
    except AttributeError:
        pass

    # User CLI args to get the data if it's missing
    try:
        if sys.argv[1]:
            ampache_url = sys.argv[1]
        if sys.argv[2]:
            ampache_api = sys.argv[2]
        if sys.argv[3]:
            ampache_user = sys.argv[3]
    except IndexError:
        pass

    # finally just ask for them if missing
    if ampache_url is None:
        ampacheConnection.AMPACHE_URL = input("Enter Ampache URL: ")
    if ampache_api is None:
        ampacheConnection.AMPACHE_KEY = input("Enter Ampache API KEY: ")
    if ampache_user is None:
        ampacheConnection.AMPACHE_USER = input("Enter Ampache USERNAME: ")

def create_stations_xml():
    # installed from deb/rpm/etc
    directory = os.path.expanduser("~/.local/share/goodvibes")
    if not os.path.exists(directory):
        print("makedir: ", directory)
        os.makedirs(directory)
    # flatpak default folder
    flatpakdirectory = os.path.expanduser("~/.var/app/io.gitlab.Goodvibes/data/goodvibes")
    if not os.path.exists(flatpakdirectory):
        print("makedir: ", flatpakdirectory)
        os.makedirs(flatpakdirectory)

    # Create the root element
    root = minidom.Document()
    stations = root.createElement("Stations")
    root.appendChild(stations)

    # xml or json are supported formats
    api_format = 'json'
    api_version = '6.0.0'

    # create an Ampache connection
    ampacheConnection = ampache.API()
    #ampacheConnection.set_debug(False)
    ampache_loader(ampacheConnection)

    # override config if it's set to xml
    ampacheConnection.set_format(api_format)

    # set the values
    ampache_url = ampacheConnection.AMPACHE_URL
    ampache_api = ampacheConnection.AMPACHE_KEY
    ampache_user = ampacheConnection.AMPACHE_USER
    ampache_session = ampacheConnection.AMPACHE_SESSION

    ping = None
    if ampache_session:
        ping = ampacheConnection.ping(ampache_url, ampache_session, api_version)
    if not ping:
        print('Connecting to:\n    ', ampache_url)
        encrypted_key = ampacheConnection.encrypt_string(ampache_api, ampache_user)
        ampache_session = ampacheConnection.handshake(ampache_url, encrypted_key, '', 0, api_version)

    if not ampache_session:
        print()
        sys.exit('ERROR: Failed to connect to ' + ampache_url)

    try:
        # Did it work? save config
        ampacheConnection.save_config()
    except AttributeError:
        pass

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
        uri.appendChild(root.createTextNode(ampache_url + "/play/ssid/" + authtoken + "/uid/" + user[
            "id"] + "/random/1/random_type/" + listtype + "/random_id/" + playlist["id"].replace("smart_", "", 1)))
        station.appendChild(uri)
        name = root.createElement("name")
        name.appendChild(root.createTextNode(playlist["name"]))
        station.appendChild(name)

    # Write the XML to file
    #with open(os.path.join(directory, "stations.xml"), "w") as f:
    #    root.writexml(f, indent="  ", newl="\n", addindent="  ", encoding="utf-8")
    # Write the XML to file
    #with open(os.path.join(flatpakdirectory, "stations.xml"), "w") as f:
    #    root.writexml(f, indent="  ", newl="\n", addindent="  ", encoding="utf-8")
    # Write the XML to file
    with open("stations.xml", "w") as f:
        root.writexml(f, indent="  ", newl="\n", addindent="  ", encoding="utf-8")


# Driver Code
if __name__ == "__main__":
    create_stations_xml()
