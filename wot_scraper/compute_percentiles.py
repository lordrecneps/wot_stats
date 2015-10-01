import psycopg2
from os import environ

conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
	environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
	environ['POSTGRES_PORT'], environ['POSTGRES_PW']
))
c = conn.cursor()

vals = [0.01*x for x in range(100)]
vals.extend([0.001*x for x in range(1, 10)])

c.execute('select distinct tank_id, tank_name from top50')
tank_ids = c.fetchall()

for tid, tname in tank_ids:
	c.execute('select percentile_cont(%s) within group(order by dpg desc nulls last) from dpg2 where tank_id = %s and battles >= 25', (vals, tid))
	
	for idx, dpg in enumerate(c.fetchall()[0][0]):
		c.execute('insert into percentiles(tank_id, dpg, pct) values(%s, %s, %s)', (tid, dpg, 1.0-vals[idx]))
	conn.commit()
	print(tname)

conn.close()