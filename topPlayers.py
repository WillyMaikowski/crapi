
import requests
import json
import sqlite3
from datetime import datetime

print( datetime.now() )

regions = []
with open( 'regions.json' ) as json_data: regions = json.load( json_data )
if len( regions ) <= 0: quit()

link = sqlite3.connect( 'DB' )
c = link.cursor()
c.execute( 'create table if not exists PLAYERS ( PLA_ID varchar(255) primary key, PLA_NAME varchar(255), PLA_SCORE integer, PLA_CLA_ID varchar(255) )' )


url = 'https://api.clashroyale.com/v1'
token = 'TOKEN'

sess = requests.session()
sess.headers.update( { 'Authorization': 'Bearer '+token } )

INTENTOS = 5
for country in regions:
	if not country['isCountry']: continue

	intentos = 0
	while intentos < INTENTOS:
		try:
			req = sess.get( url+'/locations/'+str( country['id'] )+'/rankings/players', timeout=20 )
			break
		except requests.exceptions.RequestException as e:
			intentos += 1

	if intentos >= INTENTOS:
		print( 'Fallo el pais: '+country['key'] )
		continue

	if req.status_code != requests.codes.ok or len( req.text ) <= 0:
		print( req.status_code )
		continue

	miembros = json.loads( req.text )

	tuplas = []
	for m in miembros['items']:
		clanTag = ''
		if 'clan' in m: clanTag = m['clan']['tag'][1:]

		tuplas.append( ( m['tag'][1:], m['name'], m['trophies'], clanTag ) )

	c.executemany( "insert or ignore into PLAYERS ( PLA_ID, PLA_NAME, PLA_SCORE, PLA_CLA_ID ) values ( ?, ?, ?, ? )", tuplas )


link.commit()
link.close()


print( datetime.now() )
