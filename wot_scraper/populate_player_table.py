import psycopg2
import asyncio
import aiohttp
from math import ceil
import sys
from os import environ
from common import app_ids, proxy_urls, sems, num_apps

tank_stat_url = 'http://api.worldoftanks.com/wot/account/info/'
stat_fields = ['nickname', 'statistics.random.battles']
queries = {
	'application_id': 'ca49fa564ed39d6a5af35af7725beda2',
	'fields': ','.join(stat_fields),
	'extra': 'statistics.random',
	'account_id': ''
}

conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
	environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
	environ['POSTGRES_PORT'], environ['POSTGRES_PW']
))
c = conn.cursor()

@asyncio.coroutine
def get_player_nicks(ids, app_idx):
	queries['account_id'] = ids
	queries['application_id'] = app_ids[app_idx]
	stat_url = tank_stat_url + '?' + '&'.join([k + '=' + queries[k] for k in queries])
	payload = {'a': stat_url}
	response = yield from asyncio.wait_for(aiohttp.post(proxy_urls[app_idx], data=payload), 10)
	#response = yield from aiohttp.get(tank_stat_url, params=queries)
	return (yield from response.json())

@asyncio.coroutine
def proc_account_names(id_list, results, idx):
	sem_idx = idx % num_apps
	try:
		with (yield from sems[sem_idx]):
			response = yield from get_player_nicks(id_list, sem_idx)
		
		if response['status'] == 'error':
			print('Error response received:', response)
			for account_id in [int(x) for x in id_list.split(',')]:
				c.execute('''
					insert into failed(account_id) (select * from (values (%(a)s) ) as bla
					where not exists (select 1 from failed where account_id = %(a)s))''',
					{'a': account_id}
				)
			conn.commit()
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
				'b': int(r_stats['random']['battles'])
			}
			if result['b'] >= 100:
				results.append(result)
			
	except Exception as e:
		print(e, 'Exception', end='\r')
		for account_id in [int(x) for x in id_list.split(',')]:
			c.execute('''
				insert into failed(account_id) (select * from (values (%(a)s) ) as bla
				where not exists (select 1 from failed where account_id = %(a)s))''',
				{'a': account_id}
			)
		conn.commit()
	except:
		print(sys.exc_info(), 'Exception(sys):')
		for account_id in [int(x) for x in id_list.split(',')]:
			c.execute('''
				insert into failed(account_id) (select * from (values (%(a)s) ) as bla
				where not exists (select 1 from failed where account_id = %(a)s))''',
				{'a': account_id}
			)
		conn.commit()

start_id = 1000000000
batch_size = 1000
max_account = 1016000000

proc_failed = False

player_ids = []
player_id_num = max_account - start_id

if proc_failed:
	start_id = 0
	
	c.execute('select account_id from failed')
	player_ids = c.fetchall()
	c.execute('delete from failed')
	conn.commit()
	
	player_id_num = len(player_ids)
	max_account = player_id_num


num_requests = int( ceil(player_id_num / 100) )
print('Number of accounts to process:', player_id_num)
print('Number of http requests:', num_requests)

num_batches = int( ceil(num_requests / batch_size) )
print('Number of batches:', num_batches)

for b in range(num_batches):
	start = start_id + b * 100 * batch_size
	end = start_id + (b + 1) * 100 * batch_size
	print('Batch:', b, end='\r')
	id_lists = []
	
	for b_start in range(start, end, 100):
		b_end = b_start + 100
		
		if b_end > max_account:
			b_end = max_account
		
		if proc_failed:
			id_lists.append(','.join( [str(ri[0]) for ri in player_ids[b_start:b_end]] ))
		else:
			id_lists.append(','.join( [str(ri) for ri in range(b_start, b_end)] ))
		
		if b_end == max_account:
			break
	
	acc_names = []
	loop = asyncio.get_event_loop()
	f = asyncio.wait([
		proc_account_names(id_list, acc_names, idx)
		for idx, id_list in enumerate(id_lists)
	])
	loop.run_until_complete(f)
	print('                             ', end='\r')
	print('loop: ', b, end='\r')
	
	insert_str = ['''
		insert into players2(account_id, nickname, battles)
		(select * from (values''']
	insert_vals = ','.join(c.mogrify("(%(a)s,%(n)s,%(b)s)", x).decode('utf-8') for x in acc_names)
	insert_str.append(insert_vals)
	insert_str.append(''') as c(a, n, b)
		where not exists (select 1 from players2 where account_id = c.a))''')
	c.execute(' '.join(insert_str))
	conn.commit()

conn.close()