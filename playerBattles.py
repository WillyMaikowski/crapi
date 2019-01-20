import sys
import requests
import json
import sqlite3
from datetime import datetime

if len( sys.argv ) < 2:
	print( 'Mal' )
	quit()

i = int( sys.argv[1] )
if i < 1 or i > 8:
	print( 'Mal' )
	quit()

print( datetime.now() )

link = sqlite3.connect( 'DB' )
c = link.cursor()
c.execute( 'create table if not exists BATTLES ( BAT_PLA_ID varchar(255), BAT_PLA_ID_2 varchar(255), BAT_RESULT varchar(255), BAT_TYPE varchar(255), BAT_SUBTYPE varchar(255), BAT_PLAYER_WINCOUNT integer, BAT_TIME integer, BAT_DECKS text, BAT_DECK_TYPE varchar(255), primary key ( BAT_PLA_ID, BAT_TIME ) )' )

url = 'https://api.clashroyale.com/v1'
token = 'TOKEN'


count = 0
k = 940000
j = (i-1)*k
c.execute( "select PLA_ID from PLAYERS order by PLA_ID limit "+str( j )+","+str( k ) )
players = c.fetchall()

sess = requests.session()
sess.headers.update( { 'Authorization': 'Bearer '+token } )

exc = 0
NRO_INTENTOS = 2
for (pla_id,) in players:

	intentos = 0
	while intentos < NRO_INTENTOS:
		try:
			req = sess.get( url+'/players/%23'+pla_id+'/battlelog', timeout=5 )
			break
		except requests.exceptions.RequestException as e:
			print( 'falla', pla_id, e )
			intentos += 1

	if intentos >= NRO_INTENTOS:
		exc += 1
		continue

	if req.status_code != requests.codes.ok or len( req.text ) <= 0: continue

	battles = json.loads( req.text )

	tuplas = []
	for b in battles:
		if len( b['team'] ) > 1: continue

		p1 = b['team'][0]
		p2 = b['opponent'][0]
		wincount = 0
		decksel = ''
		if 'deckSelection' in b : decksel = b['deckSelection']
		if 'challengeWinCountBefore' in b: wincount = b['challengeWinCountBefore']

		btime = ( datetime.strptime( b['battleTime'], '%Y%m%dT%H%M%S.%fZ' ) - datetime( 1970, 1, 1 ) ).total_seconds()
		decks = json.dumps(  [ p1['cards'], p2['cards'] ] )
		tuplas.append( ( p1['tag'][1:], p2['tag'][1:], str( p1['crowns'] )+'-'+str( p2['crowns'] ), b['type'], b['gameMode']['id'], wincount, btime, decks, decksel ) )


	c.executemany( "insert or ignore into BATTLES ( BAT_PLA_ID, BAT_PLA_ID_2, BAT_RESULT, BAT_TYPE, BAT_SUBTYPE, BAT_PLAYER_WINCOUNT, BAT_TIME, BAT_DECKS, BAT_DECK_TYPE ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? )", tuplas )


link.commit()
link.close()

print( datetime.now() )
print( exc )
