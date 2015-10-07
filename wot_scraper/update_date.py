import re
from datetime import timedelta
from datetime import datetime
from datetime import timezone

def main():
	curr_time = datetime.now(timezone.utc)
	est_diff = timedelta(hours=-5)
	update_time = curr_time + est_diff

	repl_html = '<span id="last_update">Last update: ' + update_time.strftime('%d %b %H:%M EST') + '</span>'

	with open('templates/master.html', 'r') as f:
		master_html = f.read()
		new_html = re.sub(r'<span id="last_update">.*?</span>', repl_html, master_html)
	
	with open('templates/master.html', 'w') as f:
		f.write(new_html)

if __name__ == "__main__":
	main()