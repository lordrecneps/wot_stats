var HttpClient = function() {
	this.get = function(aUrl, aCallback) {
		var anHttpRequest = new XMLHttpRequest();
		anHttpRequest.onreadystatechange = function() {
			if (anHttpRequest.readyState == 4 && anHttpRequest.status == 200)
				aCallback(anHttpRequest.responseText);
		}
		anHttpRequest.open( "GET", aUrl, true );            
		anHttpRequest.send( null );
	}
}


aClient = new HttpClient();
aClient.get('https://api.worldoftanks.com/wot/tanks/stats/?application_id=ca49fa564ed39d6a5af35af7725beda2&fields=tank_id%2C%20random.battles%2C%20random.damage_dealt%2Crandom.frags%2Crandom.wins&extra=random&account_id={{player_id}}', function(response) {
	var tank_json = JSON.parse(response);
	var tank_table = document.getElementById("tank_stats").tBodies[0];
	
	if(tank_json['status'] === 'ok') {
		for(var id in tank_json['data']) {
			var tank_data = tank_json['data'][id];
			tank_data.sort(function(a, b) {
				var entry_a = tank_info_map[a['tank_id']];
				var entry_b = tank_info_map[b['tank_id']]
				
				if(entry_a === undefined && entry_b == undefined)
					return 0;
				if(entry_a === undefined)
					return -1;
				if(entry_b == undefined)
					return 1;
				
				var tier_diff = entry_a[1] - entry_b[1];
				
				if(tier_diff != 0)
					return -tier_diff;
				
				var s1 = entry_a[0];
				var s2 = entry_b[0];
				return s1 < s2 ? -1 : s1 > s2 ? 1 : 0;
			});
			var tid_dpg_table = {};
			for(var i in tank_data) {
				var tid = tank_data[i]['tank_id']
				var entry = tank_info_map[tid];
				if (entry === undefined)
					continue;
				
				var random_stats = tank_data[i]['random'];
				var battles = random_stats['battles'];
				if( battles < 50 )
					continue;
				
				var row = tank_table.insertRow(-1);
				
				var idCell = row.insertCell(-1);
				idCell.innerHTML = '<a href="../tanks/' + tank_data[i]['tank_id'] + '/">' + entry[0] + '</a>';
				
				var tierCell = row.insertCell(-1);
				tierCell.textContent = entry[1];
				
				var stat_keys = ['battles', 'damage_dealt', 'frags', 'wins'];
				
				for(var stat in stat_keys) {
					var cell = row.insertCell(-1);
					
					if( stat > 0 && stat < 3 )
						cell.textContent = parseFloat(random_stats[stat_keys[stat]] / battles).toFixed(2);
					else if( stat == 3 )
						cell.textContent = parseFloat(random_stats[stat_keys[stat]] / battles * 100).toFixed(2);
					else
						cell.textContent = battles;
				}
				
				var perCell = row.insertCell(-1);
				perCell.textContent = 'one hunnit';
				perCell.id = tid;
				
				tid_dpg_table[tid] = parseFloat(random_stats['damage_dealt'] / battles).toFixed(4);
			}
			
			var pct_str = "";
			for(var k in tid_dpg_table)
			{
				pct_str += k + ":" + tid_dpg_table[k] + ',';
			}
			pct_str = pct_str.slice(0, -1);
			
			aClient = new HttpClient();
			aClient.get('http://www.dpgwhores.com/api/pct/?id=' + pct_str, function(response) {
				var pct_json = JSON.parse(response);
				
				for(var i = 0, row; row = tank_table.rows[i]; ++i)
				{
					var pct_cell = row.cells[6];
					pct_cell.textContent = parseFloat(pct_json[pct_cell.id] * 100).toFixed(2);
				}
			});
		}
	}
});