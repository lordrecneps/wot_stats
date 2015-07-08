import urllib.request as web
import json
import pprint
import sqlite3
from timeit import default_timer as timer

tank_stat_url = 'http://api.worldoftanks.com/wot/tanks/stats/'
stat_fields = ['tank_id', 'all.battles', 'all.damage_dealt', 'all.frags', 'all.spotted','all.wins']


account_id = 1000185415
queries = {
    'application_id': 'ca49fa564ed39d6a5af35af7725beda2',
    'fields': ','.join(stat_fields),
    'account_id': str(account_id)
}

start_time = timer()
tank_stats = []
players = []
for id in range(account_id, account_id + 20):
    queries['account_id'] = str(id)
    stat_url = tank_stat_url + '?' + '&'.join([k + '=' + queries[k] for k in queries])

    query_json = web.urlopen(stat_url)
    query_data = json.loads(query_json.read().decode())

    if query_data['status'] == 'error':
        print('yo', query_data)
        continue

    player_data = query_data['data']
    
    for p in player_data:
        if not player_data[p]:
            break
        players.append(tuple([int(p)]))
        for tank in player_data[p]:
            tank_stats.append( tuple([p, tank['tank_id']] + [tank['all'][k] for k in sorted(tank['all'].keys())]) )

conn = sqlite3.connect('wot_stats.db')
c = conn.cursor()

'''dpg(account_id int, tank_id int, battles int, damage_dealt int, frags int, spotted int, wins int, primary key(account_id, tank_id))'''

# Insert a row of data
c.executemany("insert or ignore into dpg values (?,?,?,?,?,?,?)", tank_stats)
c.executemany("insert or ignore into players values (?)", players)

# Save (commit) the changes
conn.commit()

'''c.execute("select * from dpg order by account_id")
for row in c:
    pprint.pprint(row)'''

conn.close()

print(timer() - start_time)