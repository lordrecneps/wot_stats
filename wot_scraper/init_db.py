import psycopg2

conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
	environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
	environ['POSTGRES_PORT'], environ['POSTGRES_PW']
))
c = conn.cursor()

c.execute('''CREATE TABLE dpg
             (account_id int, tank_id int, battles int, damage_dealt int, frags int, wins int, primary key(account_id, tank_id))''')
c.execute('''CREATE TABLE players
             (account_id int, nickname text, battles int, primary key(account_id))''')
c.execute('''create table dpg_done(account_id int, primary key(account_id))''')
c.execute('''create table tanks (tank_id int, tank_name text, primary key(tank_id))''')

c.execute('''insert into dpg_done(account_id) values(%s)''', (1000000000,))

conn.commit()
conn.close()