from os import environ
import psycopg2
import json

def main():
	page_html = '''
	{{% extends "master.html" %}}
	{{% set active_page = "tanks" %}}

	{{% block body %}}
			<section>
				<h2>Top 50 DPG Tables. 150 Battles and above only.</h2>
				Sort by: <a href="./">Alphabetically</a> |
				<a href="?sort_by=tier">Tier</a> |
				<a href="?sort_by=nation">Nation</a> |
				<a href="?sort_by=type">Class</a>
				<br />
				{tanks}
			</section>
	{{% endblock %}}
	'''

	conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
		environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
		environ['POSTGRES_PORT'], environ['POSTGRES_PW']
	))
	c = conn.cursor()
	c.execute('''select tank_id, name, nation, type, tier
				from tanks
				where tank_id in (select distinct tank_id from top50)
				order by name''')

	valid_tanks = c.fetchall()
	valid_tanks_tier = sorted(valid_tanks, key=lambda t: t[4])
	valid_tanks_nation = sorted(valid_tanks_tier, key=lambda t: t[2])
	valid_tanks_type = sorted(valid_tanks_tier, key=lambda t: t[3])

	c.execute('select distinct nation from tanks order by nation')
	nations = [nation[0] for nation in c.fetchall()]

	c.execute('select distinct type from tanks order by type')
	tank_types = [ttype[0] for ttype in c.fetchall()]

	conn.close()

	tank_html = []
	tank_html_tier = []
	tank_html_nation = []
	tank_html_class = []

	tank_html.append('<br /><ul class="tank_list">')
	for tank_id, tank_name, *_ in valid_tanks:
		tank_html.append('''<li><a href='{}/'>{}</a></li>'''.format(tank_id, tank_name))
	tank_html.append('</ul>')

	curr_tier = 0
	for tank_id, tank_name, foo, bar, tier in valid_tanks_tier:
		if curr_tier < tier:
			if curr_tier > 0:
				tank_html_tier.append('</ul>')
			curr_tier = tier;
			tank_html_tier.append('<br /><h3>Tier {}</h3>'.format(tier))
			tank_html_tier.append('<ul class="tank_list">')
		tank_html_tier.append('''<li><a href='{}/'>{}</a></li>'''.format(tank_id, tank_name))
	tank_html_tier.append('</ul>')

	for nation in nations:
		nation_entries = [(entry[0], entry[1]) for entry in valid_tanks_tier if entry[2] == nation]
		tank_html_nation.append('<br /><h3>{}</h3>'.format(nation))
		tank_html_nation.append('<ul class="tank_list">')
		
		for tank_id, tank_name in nation_entries:
			tank_html_nation.append('''<li><a href='{}/'>{}</a></li>'''.format(tank_id, tank_name))
		tank_html_nation.append('</ul>')

	type_map = {
		'AT-SPG': 'TDs',
		'heavyTank': 'Heavies',
		'mediumTank': 'Mediums',
		'lightTank': 'Lights',
		'SPG': 'Arty'
	}

	for ttype in tank_types:
		type_entries = [(entry[0], entry[1]) for entry in valid_tanks_tier if entry[3] == ttype]
		tank_html_class.append('<br /><h3>{}</h3>'.format(type_map.get(ttype, 'TENK')))
		tank_html_class.append('<ul class="tank_list">')
		
		for tank_id, tank_name in type_entries:
			tank_html_class.append('''<li><a href='{}/'>{}</a></li>'''.format(tank_id, tank_name))
		tank_html_class.append('</ul>')

	pages = {
			'tanks.html': tank_html,
			'tanks_tier.html': tank_html_tier,
			'tanks_nation.html': tank_html_nation,
			'tanks_type.html': tank_html_class
			}

	for page, content in pages.items():
		with open('templates/' + page, 'w') as f:
			page_str = ''.join(content)
			f.write(page_html.format(**{'tanks': page_str.encode('ascii', 'xmlcharrefreplace').decode('ascii')}))
			f.close()

	tank_info_js = ['var tank_info_map = ']
	tank_info_dict = dict( zip([t[0] for t in valid_tanks], [[t[1], t[4]] for t in valid_tanks]) )
	tank_info_js.append(json.dumps(tank_info_dict))

	with open('static/tank_info.js', 'w') as f:
		f.write(''.join(tank_info_js))
		f.close()