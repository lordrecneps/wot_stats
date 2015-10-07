import psycopg2
from os import environ
from datetime import timedelta
from datetime import datetime
from datetime import timezone
from common import ResultIter
from timeit import default_timer as timer




def update_table(cursor, conn, stats):
	update_str = [
	'''update dpg2 set recent_battles = c.rb, recent_dmg = c.rd, 
		recent_frags = c.rf, recent_wins = c.rw, recent_ts = c.rt, recent_dpg = c.rdpg, recent_wr = c.rwr
		from (values''']
	update_str.append(','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)", x).decode('utf-8') for x in stats))
	update_str.append(
		''') as c(rb, rd, rf, rw, rt, rdpg, rwr, a, t)
		where account_id = c.a and tank_id = c.t''')
	cursor.execute(' '.join(update_str))
	conn.commit()
	stats[:] = []

def main():
	conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
		environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
		environ['POSTGRES_PORT'], environ['POSTGRES_PW']
	))

	c = conn.cursor('hey_a_name')
	c.withhold = True
	c.itersize = 10000

	curr_time = datetime.now(timezone.utc)
	ci = conn.cursor()
	c.execute('''select account_id, tank_id, recent_ts, recent_battles, recent_dmg, recent_frags, recent_wins 
				from dpg2 where recent_dmg is not null''')
	
	start_time = timer()
	recent_results = []
	for aid, tid, *rs in ResultIter(c):
		stale_idx = -1
		rt, rb, *_ = rs
		rb_len = len(rb)
		for dt_idx, dt in enumerate(rt):
			if (curr_time - dt) / timedelta(days=1) > 90:
				stale_idx = dt_idx + 1
			else:
				break
		if stale_idx > 0:
			for s in rs:
				s[:] = s[stale_idx:]
		
		if rb:
			if len(rb) > 1 and rb[-2] < 10:
				rt[-2] = rt[-1]; del rt[-1];
				
				for s in rs[1:]:
					s[-2] += s[-1]; del s[-1];
			
			if sum(rb) - rb[0] >= 100:
				for s in rs:
					s[:] = s[1:]
			
			recent_battles = sum(rb)
			recent_dpg = sum(rs[2]) / recent_battles
			recent_wr = sum(rs[4]) / recent_battles
			recent_results.append([rb, rs[2], rs[3], rs[4], rt, recent_dpg, recent_wr, aid, tid])
		else:
			recent_results.append([None, None, None, None, None, None, None, aid, tid])
		
		if len(recent_results) >= 1000:
			update_table(ci, conn, recent_results)

	if(len(recent_results) > 0):
		update_table(ci, conn, recent_results)

	print(timer() - start_time)
	c.close()
	conn.close()

if __name__ == "__main__":
	main()