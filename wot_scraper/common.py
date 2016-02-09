import asyncio

app_ids = [
	'ca49fa564ed39d6a5af35af7725beda2',
	'74ce2b0de9bc463e29fce5910819d19a',
	'a5b203ca2e9dfbb8922f0b270c94c522',
	'ce90a5ba53cd47c6430369299e608ddf',
	'e1a40cbcc68da6b08f9c57cde3bdd691',
	'd345b43450e4d6d3c1fc91a9ae350208',
	'eeb0b7f10fcb3c374dcf792cf2270041',
	'80f6e1b0c1c7bb2a37bc564d73ae3e58',
	'749693da54ae57d1da86d38b771fbbed',
	'213dccb9b48c89fb47a796dd28170934',
]

proxy_urls = [
#	'http://proxy-dpgwhores1.rhcloud.com/',
#	'http://proxy-dpgwhores2.rhcloud.com/',
#	'http://proxy-dpgwhores3.rhcloud.com/',
#	'http://proxy-dpgwhores4.rhcloud.com/',
#	'http://proxy-dpgwhores5.rhcloud.com/',
#	'http://proxy-dpgwhores6.rhcloud.com/',
#	'http://proxy-dpgwhores7.rhcloud.com/',
#	'http://proxy-dpgwhores8.rhcloud.com/',
#	'http://proxy-dpgwhores9.rhcloud.com/',
#	'http://proxy-dpgwhores10.rhcloud.com/',
	'https://dpgw1.herokuapp.com/',
	'https://dpgw2.herokuapp.com/',
	'https://dpgw3.herokuapp.com/',
	'https://dpgw4.herokuapp.com/',
	'https://dpgw5.herokuapp.com/',
	'https://dpgw6.herokuapp.com/',
	'https://dpgw7.herokuapp.com/',
	'https://dpgw8.herokuapp.com/',
	'https://dpgw9.herokuapp.com/',
	'https://dpgw10.herokuapp.com/',
]

player_proxy_urls = [
	'https://dw11.herokuapp.com/',
	'https://dw12.herokuapp.com/',
	'https://dw13.herokuapp.com/',
	'https://dw14.herokuapp.com/',
	'https://dw15.herokuapp.com/',
	'https://dw16.herokuapp.com/',
	'https://dw17.herokuapp.com/',
	'https://dw18.herokuapp.com/',
	'https://dw19.herokuapp.com/',
	'https://dw20.herokuapp.com/',
]

sems = [asyncio.Semaphore(10) for _ in app_ids]
num_apps = len(app_ids)

def ResultIter(cursor, arraysize=1000):
	'An iterator that uses fetchmany to keep memory usage down'
	while True:
		results = cursor.fetchmany(arraysize)
		if not results:
			break
		for result in results:
			yield result

def res_array_itr(cursor, arraysize=1000):
	'An iterator that uses fetchmany to keep memory usage down'
	while True:
		results = cursor.fetchmany(arraysize)
		if not results:
			break
		yield results