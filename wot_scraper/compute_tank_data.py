import psycopg2
from os import environ
import re
import heapq
import itertools
from timeit import default_timer as timer

from common import res_array_itr

def main():
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
		conn.commit()
		print(tname)
	c.execute('drop table dpg')
	c.execute('delete from top50')
	c.execute('insert into top50 (select * from top502)')
	conn.commit()
	conn.close()
	print(timer() - start_time)
	
if __name__ == "__main__":
	main()