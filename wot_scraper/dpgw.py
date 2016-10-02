import flask
import psycopg2
import json
from flask import request
from flask import Response
from os import environ
from dpgwhores import app

# Create the application.

@app.route('/')
def index():
  server = 'na'
  if request.method == 'GET' and request.args.get('server'):
    server = request.args.get('server')
  return flask.render_template('index.html', server=server)

@app.route('/<name>/')
def dw_page(name):
  server = 'na'
  if request.method == 'GET' and request.args.get('server'):
    server = request.args.get('server')

  return flask.render_template(name + '.html', server=server)

@app.route('/tanks/', methods=['GET'])
def dw_main_tank_page():
  if request.method == 'GET':
    page = request.args.get('sort_by')

    server = request.args.get('server')
    if not server:
      server = 'na'

    valid_pages = ['nation', 'type', 'tier']
    if not page or page.lower() not in valid_pages:
      return flask.render_template(server + '/tanks.html', server=server)

    return flask.render_template(server + '/tanks_' + page + '.html', server=server)
  else:
    return flask.render_template('na/tanks.html', server='na')


@app.route('/tanks/<tank_id>/', methods=['GET'])
def dw_tank_stats(tank_id):
  if request.method == 'GET':
    page = request.args.get('v')

    server = request.args.get('server')
    if not server:
      server = 'na'

    valid_pages = ['100', '150', 'recent', '100wr', '150wr', 'recentwr']
    if not page or page.lower() not in valid_pages:
      return flask.render_template(server + '/tanks/' + tank_id + '_150.html', server=server)

    return flask.render_template(server + '/tanks/' + tank_id + '_' + page.lower() + '.html', server=server)
  else:
    return flask.render_template('na/tanks/' + tank_id + '_150.html', server='na')

def get_closest_pct(c, tid, dpg):
  c.execute(
    '''select * from
    (
      (select tank_id, pct, dpg from percentiles where dpg >= %(dpg)s and tank_id = %(id)s order by dpg limit 1)
      union all
      (select tank_id, pct, dpg from percentiles where dpg < %(dpg)s and tank_id = %(id)s order by dpg DESC limit 1)
    ) as bla
    order by abs(%(dpg)s-dpg) limit 2''', {'dpg': dpg, 'id': tid}
  )
  result = c.fetchall()
  if result and result[0]:
    if len(result) == 1:
      return result[0][1] * 100.0
    else:
      t = (dpg - result[0][2]) / (result[1][2] - result[0][2])
      pct = (1-t)*result[0][1] + t*result[1][1]
      return round(pct * 100.0, 4)
  else:
    return 0.0


@app.route('/api/pct/', methods=['GET'])
def dw_dpg_percentile():
  if request.method == 'GET':
    str_pairs = request.args.get('id')
    if not str_pairs:
      return flask.render_template('index.html')

    pairs = str_pairs.split(',')

    con_pg = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
      environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'],
      environ['POSTGRES_PORT'], environ['POSTGRES_PW']
    ))
    c_pg = con_pg.cursor()

    results = {}
    for pair in pairs:
      tid, dpg = pair.split(':')
      tid, dpg = int(tid), float(dpg)
      results[tid] = get_closest_pct(c_pg, tid, dpg)
    return Response(response=json.dumps(results), status=200, mimetype="application/json")
  else:
    return flask.render_template('index.html')


@app.route('/players/', methods=['GET'])
def dw_player_stats():
  if request.method == 'GET':
    nickname = request.args.get('player_name')
    top50_version = request.args.get('v')

    server = request.args.get('server')
    if not server:
      server = 'na'

    db_name = 'DPGWHORES_DBNAME'
    if server == 'eu':
      db_name = 'DPGWHORESEU_DBNAME'

    if not nickname:
      return flask.render_template('players.html', top50_stats = '', player_id=0, server=server)

    valid_versions = ['100', '150', 'recent', '100wr', '150wr', 'recentwr']

    query_str = '''
      select r, t50.name, tier, battles, dpg, fpg, wr, tid, rdpg, rb, rwr
      from
        (select rank() over (partition by tank_name order by {stat} desc) as r,
        account_id, name, tier, battles, dpg, fpg, wr, top50.tank_id as tid, rdpg, rb, rwr
        from top50 inner join tanks on top50.tank_id = tanks.tank_id
        where {cond}) t50
      where account_id=%s and r <=50
      order by t50.r asc
    '''
    queries = {
      '150': (query_str.format(**{'stat': 'dpg', 'cond': 'battles >= 150'}), 'DPG, 150+ battles'),
      '100': (query_str.format(**{'stat': 'dpg', 'cond': 'battles >= 100'}), 'DPG, 100+ battles'),
      'recent': (query_str.format(**{'stat': 'rdpg', 'cond': 'rb >= 35'}), 'DPG, 35+ recent battles'),
      '150wr': (query_str.format(**{'stat': 'wr', 'cond': 'battles >= 150'}), 'W/R, 150+ battles'),
      '100wr': (query_str.format(**{'stat': 'wr', 'cond': 'battles >= 100'}), 'W/R, 100+ battles'),
      'recentwr': (query_str.format(**{'stat': 'rwr', 'cond': 'rb >= 35'}), 'W/R, 35+ recent battles')
    }

    stat_query = '''
      select name, tier, battles, dpg, frags, wins, recent_battles, recent_dmg, dpg2.tank_id, recent_wins
      from dpg2 inner join tanks on dpg2.tank_id = tanks.tank_id
      where battles >= 50 and account_id=%s
      order by tier desc
    '''

    if top50_version and top50_version.lower() in valid_versions:
      top50_version = top50_version.lower()
      query = queries[top50_version]
    else:
      top50_version = '150'
      query = queries['150']

    con_pg = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
      environ[db_name], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'],
      environ['POSTGRES_PORT'], environ['POSTGRES_PW']
    ))
    c_pg = con_pg.cursor()

    c_pg.execute('select account_id, nickname from players2 where lower(nickname)=%s', (nickname.lower(), ))

    r = c_pg.fetchone()

    stats_html = [' ']

    if r:
      c_pg.execute(query[0], (r[0],))
      tank_stats = c_pg.fetchall()

      for row in tank_stats:
        stats_html.append('<tr>')

        stats_html.append('<td>{}</td>'.format(row[0]))
        stats_html.append('<td><a href="../tanks/{}/?v={}&server={}">{}</a></td>'.format(row[7], top50_version, server, row[1]))
        for i in range(2, 4):
          stats_html.append('<td>{}</td>'.format(row[i]))
        for i in range(4, 7):
          stats_html.append('<td>{0:.2f}</td>'.format(row[i]))
        if not row[9]:
          stats_html.append('<td>0</td>')
          stats_html.append('<td>0</td>')
          stats_html.append('<td>0</td>')
        else:
          stats_html.append('<td>{}</td>'.format(int(row[9])))
          stats_html.append('<td>{0:.2f}</td>'.format(row[8]))
          stats_html.append('<td>{0:.2f}</td>'.format(100.0*row[10]))
        stats_html.append('</tr>')

      c_pg.execute(stat_query, (r[0],))
      tank_stats = c_pg.fetchall()
      ps_html = [' ']
      for row in tank_stats:
        ps_html.append('<tr>')

        ps_html.append('<td><a href="../tanks/{}/?v={}&server={}">{}</a></td>'.format(row[8], top50_version, server, row[0]))
        for i in range(1, 3):
          ps_html.append('<td>{}</td>'.format(row[i]))
        ps_html.append('<td>{0:.2f}</td>'.format(row[3]))

        ps_html.append('<td>{0:.2f}</td>'.format(row[4]/row[2]))
        ps_html.append('<td>{0:.2f}</td>'.format(row[5]/row[2] * 100.0))

        ps_html.append('<td>{0:.2f}</td>'.format(get_closest_pct(c_pg, row[8], row[3])))
        #ps_html.append('<td>{0:.2f}</td>'.format(0.0))

        if not row[6]:
          ps_html.append('<td>0</td>')
          ps_html.append('<td>0</td>')
          ps_html.append('<td>0</td>')
        else:
          rb = sum(row[6])
          ps_html.append('<td>{}</td>'.format(rb))
          ps_html.append('<td>{0:.2f}</td>'.format(sum(row[7]) / rb))
          ps_html.append('<td>{0:.2f}</td>'.format(sum(row[9]) * 100.0 / rb))
        ps_html.append('</tr>')

      con_pg.close()
      return flask.render_template(
        'players.html', top50_stats=''.join(stats_html), player_name=r[1],
        player_id=r[0], player_stats=''.join(ps_html), stat_version=query[1], server=server
      )
    else:
      con_pg.close()
      return flask.render_template('players.html', top50_stats = 'Player not found.', player_id=0, server=server)
  else:
    return flask.render_template('players.html', top50_stats = '', player_id=0, server='na')

@app.route('/report/', methods=['POST'])
def dw_report_problem():
  if request.method == 'POST':
    try:
      report_type = request.form['type']
      report_msg = request.form['msg']

      con_pg = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
        environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'],
        environ['POSTGRES_PORT'], environ['POSTGRES_PW']
      ))
      c_pg = con_pg.cursor()

      c_pg.execute('''insert into reports(type, msg) values(%s, %s)''', (report_type, report_msg))

      con_pg.commit()
      con_pg.close()
    except:
      con_pg.close()

  return flask.render_template('report.html')