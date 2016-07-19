import urllib.request as web
import json
import psycopg2
from os import environ

tank_stat_url = 'http://api.worldoftanks.com/wot/encyclopedia/tanks/'
stat_fields = ['name_i18n', 'nation_i18n', 'type', 'level']

queries = {
  'application_id': '74ce2b0de9bc463e29fce5910819d19a',
  'fields': ','.join(stat_fields)
}

stat_url = tank_stat_url + '?' + '&'.join([k + '=' + queries[k] for k in queries])

conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
  environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
  environ['POSTGRES_PORT'], environ['POSTGRES_PW']
))
c = conn.cursor()

result = json.loads(web.urlopen(stat_url).readall().decode('utf-8'))

tanks = []

if result['status'] == 'error':
  print('Error: ', result)
else:
  tank_data = result['data']
  for id in tank_data:
    if not tank_data[id]:
      continue
    result = [int(id)]
    result.extend([ tank_data[id][k] for k in stat_fields ])
    tanks.append( tuple(result) );

  update_vals = ','.join(c.mogrify("(%s,%s,%s,%s,%s)", x).decode('utf-8') for x in tanks)
  update_str = ['''update tanks set name = c.name, nation = c.nation, type = c.type, tier = c.tier from (values''']
  update_str.append(update_vals)
  update_str.append(''') as c(tank_id, name, nation, type, tier) where tanks.tank_id = c.tank_id''')
  c.execute(' '.join(update_str))
  
  insert_str = ['''insert into tanks(tank_id, name, nation, type, tier) (select * from (values''']
  insert_str.append(update_vals)
  insert_str.append(''') as c(tank_id, name, nation, type, tier) where not exists (select 1 from tanks where tanks.tank_id = c.tank_id ))''')
  c.execute(' '.join(insert_str))
        
  #c.executemany('''insert into tanks(tank_id, name, nation, type, tier) values(%s,%s,%s,%s,%s)
  #                 on conflict (tank_id) do update set name = excluded.name, nation = excluded.nation, type = excluded.type, tier = excluded.tier''', tanks)
  conn.commit()

conn.close()