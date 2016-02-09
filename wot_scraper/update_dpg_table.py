import psycopg2
import asyncio
import aiohttp
from math import ceil
from timeit import default_timer as timer
from os import environ
import time
import sys
from common import app_ids, proxy_urls, sems, num_apps

tank_stat_url = 'http://api.worldoftanks.com/wot/tanks/stats/'
stat_fields = ['tank_id', 'random.battles', 'random.damage_dealt', 'random.frags', 'random.wins']

queries = {
	'application_id': app_ids[0],
	'fields': ','.join(stat_fields),
	'extra': 'random',
	'account_id': 1000000000
}

@asyncio.coroutine
def get_tank_data_proxy(account_id, app_idx):
	queries['account_id'] = str(account_id)
	queries['application_id'] = app_ids[app_idx]
	stat_url = tank_stat_url + '?' + '&'.join([k + '=' + queries[k] for k in queries])
	payload = {'a': stat_url}
	response = yield from asyncio.wait_for(aiohttp.post(proxy_urls[app_idx], data=payload), 10)
	#response = yield from aiohttp.post('http://127.0.0.1:5000/', data=payload)
	return (yield from response.json())

@asyncio.coroutine
def get_tank_data(account_id, app_idx):
	queries['account_id'] = str(account_id)
	queries['application_id'] = app_ids[app_idx]
	response = yield from aiohttp.get(tank_stat_url, params=queries)
	return (yield from response.json())

@asyncio.coroutine
def iterate_tank_data(account_id, tank_stats, idx, c, conn):
	sem_idx = idx % num_apps
	try:
		with (yield from sems[sem_idx]):
			query_json = yield from get_tank_data_proxy(account_id, sem_idx)
		proc_tank_data(query_json, account_id, tank_stats, c, conn)
	except Exception as e:
		print(e, 'Exception:', account_id, end='\r')
		c.execute('''
			insert into failed(account_id) (select * from (values (%(a)s) ) as bla
			where not exists (select 1 from failed where account_id = %(a)s))''',
			{'a': account_id}
		)
		conn.commit()
	except:
		print(sys.exc_info(), 'Exception(sys):', account_id)
		c.execute('''
			insert into failed(account_id) (select * from (values (%(a)s) ) as bla
			where not exists (select 1 from failed where account_id = %(a)s))''',
			{'a': account_id}
		)
		conn.commit()

def proc_tank_data(query_data, account_id, tank_stats, c, conn):
	if query_data['status'] == 'error':
		print('Error response received:', query_data, account_id)
		c.execute('''
			insert into failed(account_id) (select * from (values (%(a)s) ) as bla
			where not exists (select 1 from failed where account_id = %(a)s))''',
			{'a': account_id}
		)
		conn.commit()
		return

	player_data = query_data['data']

	for id in player_data:
		if not player_data[id]:
			break
		
		for tank in player_data[id]:
			result = {'a': int(id), 't': int(tank['tank_id'])}
			result['b'] = int(tank['random']['battles'])
			result['d'] = int(tank['random']['damage_dealt'])
			result['f'] = int(tank['random']['frags'])
			result['w'] = int(tank['random']['wins'])
			
			if int(result['b']) > 0:
				tank_stats.append( result )

def update_loop(c, conn, wakeup=False, proc_fails=False):
	start_id = 1000000000
	batch_size = 1000

	c.execute("select max(account_id) from dpg_done")
	start_id = c.fetchone()[0]

	if proc_fails:
		c.execute("select account_id from failed")
	else:
		c.execute("select account_id from players2 where account_id > %s", (start_id,))
	
	acc_ids = c.fetchall()
	if proc_fails:
		if not acc_ids:
			return 1
		
		c.execute("delete from failed");
		conn.commit()

	num_acc_proc = len(acc_ids)
	num_batches = int( ceil(num_acc_proc / batch_size) )

	print('Number of accounts to process:', num_acc_proc)
	print('Number of batches:', num_batches)

	start_time = timer()
	for b in range(num_batches):
		start = b*batch_size
		end = (b + 1)*batch_size
		print('Batch:', b, end='\r')
		if end > num_acc_proc:
			end = num_acc_proc
		
		tank_stats = []
		
		loop = asyncio.get_event_loop()
		f = asyncio.wait([
			iterate_tank_data( account_id[0], tank_stats, idx, c, conn ) 
			for idx, account_id in enumerate( acc_ids[start:end] )
		])
		loop.run_until_complete(f)
		print('                             ', end='\r')
		print('loop: ', b, end='\r')
		
		if wakeup:
			c.execute('delete from failed')
			conn.commit()
			return 0
		
		if tank_stats:
			update_str = ['''update dpg2 set 
					battles = c.b, dmg = c.d, frags = c.f, wins = c.w,
					recent_battles = case when battles < c.b then recent_battles || (c.b - battles) else recent_battles end,
					recent_dmg = case when battles < c.b then recent_dmg || (c.d - dmg) else recent_dmg end,
					recent_frags = case when battles < c.b then recent_frags || (c.f - frags) else recent_frags end,
					recent_wins = case when battles < c.b then recent_wins || (c.w - wins) else recent_wins end,
					recent_ts = case when battles < c.b then recent_ts || current_timestamp else recent_ts end,
					dpg = c.d::real / c.b
				from (values''']
			update_vals = ','.join(c.mogrify("(%(a)s, %(t)s, %(b)s, %(d)s, %(f)s, %(w)s)", x).decode('utf-8') for x in tank_stats)
			update_str.append(update_vals)
			update_str.append(''') as c(a, t, b, d, f, w)
				where account_id = c.a and tank_id = c.t''')
				
			c.execute(' '.join(update_str))

			insert_str = ['''insert into dpg2(
					account_id, tank_id, battles, dmg, frags, wins,
					recent_battles, recent_dmg, recent_frags, recent_wins, recent_ts, dpg)
				(select * from (values''']
			insert_vals = ','.join(c.mogrify("(%(a)s,%(t)s,%(b)s,%(d)s,%(f)s,%(w)s,'{%(b)s}'::integer[],'{%(d)s}'::integer[],'{%(f)s}'::integer[],'{%(w)s}'::integer[], '{}'::timestamp with time zone[] || current_timestamp, %(d)s::real / %(b)s)", x).decode('utf-8') for x in tank_stats)
			insert_str.append(insert_vals)
			insert_str.append(''') as c(a, t, b, d, f, w, rb, rd, rf, rw, rt, dpg)
					where not exists (select 1 from dpg2 where account_id = c.a and tank_id = c.t))''')

			c.execute(' '.join(insert_str))
			
		c.execute('insert into dpg_done(account_id) values(%s)', acc_ids[end - 1])
		
		conn.commit()

	print(timer() - start_time)
	return 0

def update_dpg():
	conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
		environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
		environ['POSTGRES_PORT'], environ['POSTGRES_PW']
	))
	c = conn.cursor()
	
	print("Waking proxies up")
	update_loop(c, conn, wakeup=True)
	time.sleep(10)
	print("Second waking")
	update_loop(c, conn, wakeup=True)
	time.sleep(5)
	
	done = False
	
	for _ in range(10):
		try:
			print("Starting main update")
			conn.commit()
			update_loop(c, conn)
			print("Finished main update")
			done = True
			break
		except:
			print(sys.exc_info(), 'Exception(sys)')
	
	if done:
		done = False
		print("Processing fails")
		for _ in range(10):
			try:
				for _ in range(10):
					conn.commit()
					if update_loop(c, conn, proc_fails=True) == 1:
						break
				done = True
				break
			except:
				print(sys.exc_info(), 'Exception(sys)')
	
	if done:
		c.execute('delete from dpg_done where account_id > 1')
	conn.commit()
	conn.close()
	
	if done:
		return 1
	else:
		return 0

if __name__ == "__main__":
	exit(update_dpg())