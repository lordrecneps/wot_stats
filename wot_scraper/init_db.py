import sqlite3

conn = sqlite3.connect('wot_stats.db')
c = conn.cursor()

# Create table
#c.execute('''CREATE TABLE dpg
#             (account_id int, tank_id int, battles int, damage_dealt int, frags int, spotted int, wins int, primary key(account_id, tank_id))''')
c.execute('''CREATE TABLE players
             (account_id int, primary key(account_id))''')

# Insert a row of data
#c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()