import psycopg2
import aiohttp
import asyncio
from math import ceil
from os import environ
import sys
from timeit import default_timer as timer
import time
import threading

from common import app_ids, player_proxy_urls, sems, num_apps

lock = threading.Lock()

tank_stat_url = 'http://api.worldoftanks.com/wot/account/info/'
stat_fields = ['nickname', 'statistics.random.battles', 'last_battle_time']
queries = {
  'application_id': 'ca49fa564ed39d6a5af35af7725beda2',
  'fields': ','.join(stat_fields),
  'extra': 'statistics.random',
  'account_id': ''
}

@asyncio.coroutine
def get_player_nicks(ids, app_idx):
  queries['account_id'] = ids
  queries['application_id'] = app_ids[app_idx]
  stat_url = tank_stat_url + '?' + '&'.join([k + '=' + queries[k] for k in queries])

  response = yield from asyncio.wait_for(aiohttp.get(tank_stat_url, params=queries), 10)
  return (yield from response.json())

@asyncio.coroutine
def proc_account_names(id_list, results, id_done, idx, c, conn):
  sem_idx = idx % 1 # num_apps
  try:
    with (yield from sems[sem_idx]):
      response = yield from get_player_nicks(id_list, sem_idx)

    if response['status'] == 'error':
      print('Error response received:', response)
      return

    res_data = response['data']
    for id in res_data:
      if not res_data[id]:
        continue
      res = res_data[id]
      r_stats = res['statistics']

      result = {
        'a': int(id),
        'n': res['nickname'],
        'b': int(r_stats['random']['battles']),
        'l': int(res['last_battle_time'])
      }
      with lock:
        if result['a'] in id_done:
          continue
        if result['b'] >= 250:
          results.append(result)
          id_done.add(result['a'])

  except Exception as e:
    print(e, 'Exception')
  except:
    print(sys.exc_info(), 'Exception(sys):')

def update_loop(c, conn, start_point):
  start_id = start_point[0]
  batch_size = 1000
  c.execute('select max(account_id) from players2')
  max_account = c.fetchone()

  if not max_account:
    max_account = 1023000000
  else:
    max_account = max(max_account[0], 1023000000)

  max_account = max_account + 250000
  player_id_num = max_account - start_id

  num_requests = int( ceil(player_id_num / 100) )
  print('Number of accounts to process:', player_id_num)
  print('Number of http requests:', num_requests)

  num_batches = int( ceil(num_requests / batch_size) )
  print('Number of batches:', num_batches)

  start_time = timer()
  for b in range(num_batches):
    start = start_id + b * 100 * batch_size
    end = start_id + (b + 1) * 100 * batch_size
    print('Batch:', b, end='\r')
    id_lists = []

    for b_start in range(start, end, 100):
      b_end = b_start + 100

      if b_end > max_account:
        b_end = max_account

      id_lists.append(','.join( [str(ri) for ri in range(b_start, b_end)] ))

      if b_end == max_account:
        break

    acc_names = []
    id_done = set()
    loop = asyncio.get_event_loop()
    f = asyncio.wait([
      proc_account_names(id_list, acc_names, id_done, idx, c, conn)
      for idx, id_list in enumerate(id_lists)
    ])
    loop.run_until_complete(f)
    print('                             ', end='\r')
    print('loop: ', b, end='\r')

    if len(acc_names) > 0:
      insert_str = ['''
        insert into players2(account_id, nickname, battles)
        (select * from (values''']
      insert_vals = ','.join(c.mogrify("(%(a)s,%(n)s,%(b)s)", x).decode('utf-8') for x in acc_names)
      insert_str.append(insert_vals)
      insert_str.append(''') as c(a, n, b)
        where not exists (select 1 from players2 where account_id = c.a))''')
      c.execute(' '.join(insert_str))
      conn.commit()
    start_point[0] = end

  print(timer() - start_time)

def update_players():
  conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
    environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'],
    environ['POSTGRES_PORT'], environ['POSTGRES_PW']
  ))
  c = conn.cursor()
  done = False
  start_point = [1000000000]

  for _ in range(10):
    try:
      print("Starting player update")
      conn.commit()
      update_loop(c, conn, start_point)
      print("Finished player update")
      done = True
      break
    except:
      print(sys.exc_info(), 'Exception(sys)')

  conn.commit()
  conn.close()

  if done:
    return 1
  else:
    return 0

if __name__ == "__main__":
  update_players()