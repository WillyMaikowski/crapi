
import requests
import json
import sqlite3
from datetime import datetime

print( datetime.now() )

link = sqlite3.connect( 'DB' )
c = link.cursor()

url = 'https://api.clashroyale.com/v1'
token = 'TOKEN'


c.execute( "select distinct BAT_PLA_ID_2 from BATTLES where BAT_PLA_ID_2 not in ( select PLA_ID from PLAYERS )" )
players = c.fetchall()

sess = requests.session()
sess.headers.update( { 'Authorization': 'Bearer '+token } )

exc = 0
tuplas = []
NRO_INTENTOS = 2
for (pla_id,) in players:

	intentos = 0
	while intentos < NRO_INTENTOS:
		try:
			req = sess.get( url+'/players/%23'+pla_id, timeout=5 )
			break
		except requests.exceptions.RequestException as e:
			print( 'falla', pla_id, e )
			intentos += 1

	if intentos >= NRO_INTENTOS:
		exc += 1
		continue

	if req.status_code != requests.codes.ok or len( req.text ) <= 0: continue

	p = json.loads( req.text )

	clanTag = ''
	if 'clan' in p: clanTag = p['clan']['tag'][1:]

	tuplas.append( ( p['tag'][1:], p['name'], p['trophies'], clanTag ) )

	if len( tuplas ) > 10000:
		c.executemany( "insert or ignore into PLAYERS ( PLA_ID, PLA_NAME, PLA_SCORE, PLA_CLA_ID ) values ( ?, ?, ?, ? )", tuplas )
		tuplas = []
		print( '.' )

if len( tuplas ) > 0:
	c.executemany( "insert or ignore into PLAYERS ( PLA_ID, PLA_NAME, PLA_SCORE, PLA_CLA_ID ) values ( ?, ?, ?, ? )", tuplas )


link.commit()
link.close()

print( datetime.now() )
print( exc )
