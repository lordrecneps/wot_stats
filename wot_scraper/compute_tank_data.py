import psycopg2
from os import environ
import re
import heapq
import itertools
from timeit import default_timer as timer

from common import res_array_itr

conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
	environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
	environ['POSTGRES_PORT'], environ['POSTGRES_PW']
))
c = conn.cursor()

c.execute('drop table if exists top502')
c.execute('''
	create table top502(account_id int, tank_id int, nickname text, tank_name text,
	battles int, dpg real, fpg real, wr real, rdpg real, rfpg real, rwr real, rb int,
	primary key(account_id, tank_id))''')
conn.commit()

c.execute('''
	create table dpg as
		select * from dpg2
		where battles >= 100 or (select sum(s) from unnest(recent_battles) s) >= 35''')
conn.commit()

c.execute('create index tid_dpg_idx on dpg(tank_id, dpg desc nulls last)')
c.execute('create index tid_rdpg_idx on dpg(tank_id, recent_dpg desc nulls last)')
c.execute('create index tid_wr_idx on dpg(tank_id, (wins::real/battles::real) desc nulls last)')
c.execute('create index tid_rwr_idx on dpg(tank_id, recent_wr desc nulls last)')
conn.commit()

c.execute('select tank_id, name from tanks where name is not null')
tank_ids = c.fetchall()

start_time = timer()
for tid, tname in tank_ids:
	'''dpg150 = []
	dpg100 = []
	dpgr = []
	wr150 = []
	wr100 = []
	wrr = []'''
	
	#c.execute(
	'''
		select dpg2.account_id, tank_id, (select nickname from players2 where players2.account_id = dpg2.account_id), %(n)s, dpg2.battles, dpg,
		frags*1.0/dpg2.battles, wins*100.0/dpg2.battles, dpg2.recent_dpg, dpg2.recent_wr,
		(select sum(s) from unnest(recent_battles) s) as rb
		from dpg2
		where tank_id=%(i)s'''
		#, {'n': tname, 'i': tid})
	
	'''for res in res_array_itr(c, 10000):
		dpg150.extend([v for v in res if v[4] >= 150])
		dpg100.extend([v for v in res if v[4] >= 100])
		dpgr.extend([v for v in res if v[10] and v[10] >= 35])
		wr150.extend([v for v in res if v[4] >= 150])
		wr100.extend([v for v in res if v[4] >= 100])
		wrr.extend([v for v in res if v[10] and v[10] >= 35])
		
		dpg150 = heapq.nlargest(50, dpg150, lambda x: x[5])
		dpg100 = heapq.nlargest(50, dpg100, lambda x: x[5])
		dpgr = heapq.nlargest(50, dpgr, lambda x: x[8])
		
		wr150 = heapq.nlargest(50, wr150, lambda x: x[7])
		wr100 = heapq.nlargest(50, wr100, lambda x: x[7])
		wrr = heapq.nlargest(50, wrr, lambda x: x[9])'''
	
	c.execute(
	'''(select dpg.account_id, tank_id, (select nickname from players2 where players2.account_id = dpg.account_id), %(n)s, dpg.battles, dpg,
				 frags*1.0/dpg.battles, wins*100.0/dpg.battles, dpg.recent_dpg, dpg.recent_wr,
				 (select sum(s) from unnest(recent_battles) s) as rb
				 from dpg
				 where tank_id=%(i)s and dpg.battles >= 150
				 order by dpg desc nulls last
				 limit 50)
				 union
				 (select dpg.account_id, tank_id, (select nickname from players2 where players2.account_id = dpg.account_id), %(n)s, dpg.battles, dpg,
				 frags*1.0/dpg.battles, wins*100.0/dpg.battles, dpg.recent_dpg, dpg.recent_wr,
				 (select sum(s) from unnest(recent_battles) s) as rb
				 from dpg
				 where tank_id=%(i)s and dpg.battles >= 150
				 order by (wins::double precision / battles::real) desc nulls last
				 limit 50)
				 union
				 (select dpg.account_id, tank_id, (select nickname from players2 where players2.account_id = dpg.account_id), %(n)s, dpg.battles, dpg,
				 frags*1.0/dpg.battles, wins*100.0/dpg.battles, dpg.recent_dpg, dpg.recent_wr,
				 (select sum(s) from unnest(recent_battles) s) as rb
				 from dpg
				 where tank_id=%(i)s and dpg.battles >= 100
				 order by dpg desc nulls last
				 limit 50)
				 union
				 (select dpg.account_id, tank_id, (select nickname from players2 where players2.account_id = dpg.account_id), %(n)s, dpg.battles, dpg,
				 frags*1.0/dpg.battles, wins*100.0/dpg.battles, dpg.recent_dpg, dpg.recent_wr,
				 (select sum(s) from unnest(recent_battles) s) as rb
				 from dpg
				 where tank_id=%(i)s and dpg.battles >= 100
				 order by (wins::double precision / battles::real) desc nulls last
				 limit 50)
				 union
				 (select dpg.account_id, tank_id, (select nickname from players2 where players2.account_id = dpg.account_id), %(n)s, dpg.battles, dpg,
				 frags*1.0/dpg.battles, wins*100.0/dpg.battles, dpg.recent_dpg, dpg.recent_wr,
				 (select sum(s) from unnest(recent_battles) s) as rb
				 from dpg
				 where tank_id=%(i)s and (select sum(s) from unnest(recent_battles) s) >= 35
				 order by recent_dpg desc nulls last
				 limit 50)
				 union
				 (select dpg.account_id, tank_id, (select nickname from players2 where players2.account_id = dpg.account_id), %(n)s, dpg.battles, dpg,
				 frags*1.0/dpg.battles, wins*100.0/dpg.battles, dpg.recent_dpg, dpg.recent_wr,
				 (select sum(s) from unnest(recent_battles) s) as rb
				 from dpg
				 where tank_id=%(i)s and (select sum(s) from unnest(recent_battles) s) >= 35
				 order by recent_wr desc nulls last
				 limit 50)
				 '''
				 , {'n': tname, 'i': tid})
	res = c.fetchall()
	c.executemany('insert into top502 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s)', res)
	#res_set = set(itertools.chain(dpg150, dpg100, dpgr, wr150, wr100, wrr))
	#c.executemany('insert into top502 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s)', list(res_set))
	conn.commit()
	print(tname)
c.execute('drop table top50')
c.execute('drop table dpg')
c.execute('alter table top502 rename to top50')
conn.commit()
conn.close()
print(timer() - start_time)