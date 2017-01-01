import re
from datetime import timedelta
from datetime import datetime
from datetime import timezone

def main(server='na'):
	curr_time = datetime.now(timezone.utc)
	if server == 'na':
		est_diff = timedelta(hours=-5)
	else:
		est_diff = timedelta(hours=0)
	update_time = curr_time + est_diff

	if server == 'na':
		repl_html = '<span id="last_update">Last update NA: ' + update_time.strftime('%d %b %H:%M EST') + ' | EU'
	else:
		repl_html = '| EU: ' + update_time.strftime('%d %b %H:%M GMT') + '</span>'

	with open('templates/master.html', 'r') as f:
		master_html = f.read()

		if server == 'na':
			new_html = re.sub(r'<span id="last_update">.*?\| EU', repl_html, master_html)
		else:
			new_html = re.sub(r'\| EU.*?</span>', repl_html, master_html)
	
	with open('templates/master.html', 'w') as f:
		f.write(new_html)

if __name__ == "__main__":
	main()
