import urllib.request as web
import json
from common import pg_connect

# Unfortunately we need to use the deprecated encyclopedia/tanks API since new tanks like the Chrysler haven't been
# added to the 'new' API
tank_stat_urls = {'na': ['http://api.worldoftanks.com/wot/encyclopedia/tanks/',
                         'http://api.worldoftanks.com/wot/encyclopedia/vehicles/'],
                  'eu': ['http://api.worldoftanks.eu/wot/encyclopedia/tanks/',
                         'http://api.worldoftanks.eu/wot/encyclopedia/vehicles/']}
stat_fields = ['name', 'nation', 'type', 'tier']
deprecated_stat_fields = ['name_i18n', 'nation_i18n', 'type', 'level']

queries = {
  'application_id': '74ce2b0de9bc463e29fce5910819d19a',
  'fields': ','.join(stat_fields)
}

deprecated_queries = {
  'application_id': '74ce2b0de9bc463e29fce5910819d19a',
  'fields': ','.join(deprecated_stat_fields)
}

def main(server='na'):
    tank_stat_url = tank_stat_urls[server]

    conn, c = pg_connect()

    stat_url = tank_stat_url[0] + '?' + '&'.join([k + '=' + deprecated_queries[k] for k in deprecated_queries])
    deprecated_result = json.loads(web.urlopen(stat_url).readall().decode('utf-8'))

    stat_url = tank_stat_url[1] + '?' + '&'.join([k + '=' + queries[k] for k in queries])
    result = json.loads(web.urlopen(stat_url).readall().decode('utf-8'))

    tanks = []

    if result['status'] == 'error':
        print('Error: ', result)
    else:
        tank_data = result['data']

        for tank_id in tank_data:
            if not tank_data[tank_id]:
                continue
            result = [int(tank_id)]
            result.extend([ tank_data[tank_id][k] for k in stat_fields ])
            tanks.append(tuple(result))

        if deprecated_result['status'] == 'ok':
            deprecated_data = deprecated_result['data']
            for tank_id in deprecated_data:
                if tank_id in tank_data or not deprecated_data[tank_id]:
                    continue
                result = [int(tank_id)]
                result.extend([ deprecated_data[tank_id][k] for k in deprecated_stat_fields ])
                tanks.append(tuple(result))


        update_vals = ','.join(c.mogrify("(%s,%s,%s,%s,%s)", x).decode('utf-8') for x in tanks)
        update_str = ['''update tanks set name = c.name, nation = c.nation, type = c.type, tier = c.tier from (values''']
        update_str.append(update_vals)
        update_str.append(''') as c(tank_id, name, nation, type, tier) where tanks.tank_id = c.tank_id''')
        c.execute(' '.join(update_str))

        insert_str = ['''insert into tanks(tank_id, name, nation, type, tier) (select * from (values''']
        insert_str.append(update_vals)
        insert_str.append(''') as c(tank_id, name, nation, type, tier) where not exists (select 1 from tanks where tanks.tank_id = c.tank_id ))''')
        c.execute(' '.join(insert_str))
        conn.commit()

    conn.close()