import urllib.request as web
import json
import pprint

jcrap = web.urlopen("http://api.worldoftanks.com/wot/tanks/stats/?application_id=ca49fa564ed39d6a5af35af7725beda2&fields=tank_id%2C%20all.damage_dealt&account_id=1000185415")
jpee = json.loads(jcrap.read().decode())
pprint.pprint(jpee)