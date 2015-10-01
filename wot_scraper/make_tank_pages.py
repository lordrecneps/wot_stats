page_html = '''
{{% extends "tank.html" %}}
{{% block title %}}{tank_name}{{% endblock %}}
{{% block tname %}}{tank_name}{{% endblock %}}
{{% block stat_version %}}{stat_version}{{% endblock %}}
{{% block top50_body %}}
{rows}
{{% endblock %}}

'''

from os import environ
import psycopg2

conn = psycopg2.connect("dbname='{}' user='{}' host='{}' port={} password='{}'".format(
	environ['DPGWHORES_DBNAME'], environ['POSTGRES_USERNAME'], environ['POSTGRES_HOST'], 
	environ['POSTGRES_PORT'], environ['POSTGRES_PW']
))
c = conn.cursor()

c.execute('select distinct tank_id, tank_name from top50')
valid_tanks = c.fetchall()

query_strs = [
	('''select nickname, battles, dpg, fpg, wr, case when rb > 0 then rdpg else null end, rb, rwr
					from top50
					where tank_id=%s and battles >= 150
					order by dpg desc nulls last
					limit 50;''', '150', 'DPG (150+ battles)'),
	('''select nickname, battles, dpg, fpg, wr, case when rb > 0 then rdpg else null end, rb, rwr
					from top50
					where tank_id=%s and battles >= 100
					order by dpg desc nulls last
					limit 50;''', '100', 'DPG (100+ battles)'),
	('''select nickname, battles, dpg, fpg, wr, rdpg, rb, rwr
					from top50
					where tank_id=%s and rb >= 35
					order by rdpg desc nulls last
					limit 50;''', 'recent', 'DPG (35+ recent battles)'),
	('''select nickname, battles, dpg, fpg, wr, case when rb > 0 then rdpg else null end, rb, rwr
					from top50
					where tank_id=%s and battles >= 150
					order by wr desc nulls last
					limit 50;''', '150wr', 'W/R (150+ battles)'),
	('''select nickname, battles, dpg, fpg, wr, case when rb > 0 then rdpg else null end, rb, rwr
					from top50
					where tank_id=%s and battles >= 100
					order by wr desc nulls last
					limit 50;''', '100wr', 'W/R (100+ battles)'),
	('''select nickname, battles, dpg, fpg, wr, rdpg, rb, rwr
					from top50
					where tank_id=%s and rb >= 35
					order by rwr desc nulls last
					limit 50;''', 'recentwr', 'W/R (35+ recent battles)')
]

for query_str, html_ext, stat_version in query_strs:
	for tank_id, tank_name in valid_tanks:
			c.execute(query_str, (tank_id,));
			rows = c.fetchall()
			
			row_html = []
			
			for idx, row in enumerate(rows, 1):
				row_html.append('<tr>')
				row_html.append('<td>{}</td>'.format(idx))
				
				row_html.append('<td><a href="../../players/?player_name={}&v={}">{}</a></td>'.format(row[0], html_ext, row[0]))
				
				row_html.append('<td>{}</td>'.format(row[1]))
				for i in range(2, 5):
					row_html.append('<td>{0:.2f}</td>'.format(row[i]))
				if not row[5]:
					row_html.append('<td>0</td>')
					row_html.append('<td>0</td>')
					row_html.append('<td>0</td>')
				else:
					row_html.append('<td>{}</td>'.format(int(row[6])))
					row_html.append('<td>{0:.2f}</td>'.format(row[5]))
					row_html.append('<td>{0:.2f}</td>'.format(100.0*row[7]))
					
				row_html.append('</tr>')
			
			with open('templates/tanks/{}_{}.html'.format(tank_id, html_ext), 'w') as f:
				page_str = ''.join(row_html)
				final_html = page_html.format(**{'tank_name': tank_name, 'rows': page_str, 'stat_version': stat_version})
				final_html = final_html.encode('ascii', 'xmlcharrefreplace').decode('ascii')
				f.write(final_html)
				f.close()

conn.close()