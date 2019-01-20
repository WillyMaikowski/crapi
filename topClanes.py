
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
c.execute( 'create table if not exists CLANES ( CLA_ID varchar(255) primary key, CLA_NAME varchar(255), CLA_SCORE integer, CLA_LOC_ID varchar(255) )' )


url = 'https://api.clashroyale.com/v1'
token = 'TOKEN'

sess = requests.session()
sess.headers.update( { 'Authorization': 'Bearer '+token } )

INTENTOS = 5
for country in regions:

	intentos = 0
	while intentos < INTENTOS:
		try:
			req = sess.get( url+'/locations/'+str( country['id'] )+'/rankings/clans', timeout=20 )
			break
		except requests.exceptions.RequestException as e:
			intentos += 1

	if intentos >= INTENTOS:
		print( 'Fallo el pais: '+country['key'] )
		continue

	if req.status_code != requests.codes.ok or len( req.text ) <= 0: continue

	clanes = json.loads( req.text )

	tuplas = []
	for clan in clanes['items']:
		tuplas.append( ( clan['tag'][1:], clan['name'], clan['clanScore'], country['key'] ) )

	c.executemany( "insert or ignore into CLANES ( CLA_ID, CLA_NAME, CLA_SCORE, CLA_LOC_ID ) values ( ?, ?, ?, ? )", tuplas )


link.commit()
link.close()


print( datetime.now() )
