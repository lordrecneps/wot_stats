import urllib.request as web
import json
import pprint
import sqlite3
import asyncio
import aiohttp
from timeit import default_timer as timer

tank_stat_url = 'http://api.worldoftanks.com/wot/tanks/stats/'
stat_fields = ['tank_id', 'all.battles', 'all.damage_dealt', 'all.frags', 'all.spotted','all.wins']


start_id = 1000000000
queries = {
    'application_id': 'ca49fa564ed39d6a5af35af7725beda2',
    'fields': ','.join(stat_fields),
    'account_id': str(start_id)
}

start_time = timer()
tank_stats = []
players = []
sem = asyncio.Semaphore(10)

conn = sqlite3.connect('wot_stats.db')
c = conn.cursor()

@asyncio.coroutine
def get_tank_data(url):
    response = yield from aiohttp.request('GET', url)
    return (yield from response.read_and_close(decode=True))


def commit_data():
    global c, tank_stats, players
    '''dpg(account_id int, tank_id int, battles int, damage_dealt int, frags int, spotted int, wins int, primary key(account_id, tank_id))'''

    # Insert a row of data
    c.executemany("insert or ignore into dpg values (?,?,?,?,?,?,?)", tank_stats[:10000])
    c.executemany("insert or ignore into players values (?)", players[:10000])

    tank_stats[:10000] = []
    players[:10000] = []

    # Save (commit) the changes
    conn.commit()

    '''c.execute("select * from dpg order by account_id")
    for row in c:
        pprint.pprint(row)'''


def proc_tank_data(query_data, account_id):
    global tank_stats
    global players
    
    if query_data['status'] == 'error':
        print('yo', query_data)
        return

    player_data = query_data['data']
    
    for p in player_data:
        if not player_data[p]:
            break
        players.append(tuple([int(p)]))
        for tank in player_data[p]:
            tank_stats.append( tuple([p, tank['tank_id']] + [tank['all'][k] for k in sorted(tank['all'].keys())]) )

    if len(players) >= 10000:
        commit_data()


@asyncio.coroutine
def iterate_tank_data(account_id):
    #print(account_id)
    queries['account_id'] = str(account_id)

    stat_url = tank_stat_url + '?' + '&'.join([k + '=' + queries[k] for k in queries])
    
    with (yield from sem):
        query_json = yield from get_tank_data(stat_url)
    proc_tank_data(query_json, account_id)

num_acc_proc = 10000000
batch_size = 50000
max_account = 1015000000

c.execute("select max(account_id) from players");
start_id = c.fetchone()[0]

for b in range(num_acc_proc // batch_size):
    if start_id + b*batch_size >= max_account:
        break

    loop = asyncio.get_event_loop()
    f = asyncio.wait([iterate_tank_data(account_id) for account_id in range(start_id + b*batch_size, start_id + (b + 1)*batch_size)])
    loop.run_until_complete(f)

commit_data()
conn.close()

print(timer() - start_time)