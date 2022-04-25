#! /usr/bin/python3
import requests
import os
from bs4 import BeautifulSoup
import json
import random
import time
import re
import csv


def renew_connection():
	import stem
	import stem.connection
	from stem import Signal
	from stem.control import Controller

	with Controller.from_port(port = 9051) as controller:
		controller.authenticate(password = 'atmega')
		controller.signal(Signal.NEWNYM)
		controller.close()
		session = requests.session()
		session.proxies = {}
		session.proxies['http'] = 'socks5h://localhost:9050'
		session.proxies['https'] = 'socks5h://localhost:9050'
		# newIP = session.get("http://icanhazip.com/").text
		return session


def getStateZIPcodes(state):
	mainurl='https://www.unitedstateszipcodes.org/'
	stateURL=state.get('StateURL')
	stateName=state.get('StateName')
	user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
	headers={'User-Agent':user_agent}
	with requests.get(stateURL,headers=headers) as req:
		if req.status_code == 200:
			bsobj=BeautifulSoup(req.text,'html.parser')
			# tableHeaders=bsobj.find('div',{'class':'panel-heading visible-md visible-lg'}).div.findAll('div')
			tableHeaders=['ZIP Code','Type','Common Cities','County','Area Codes','ZIPURL','StateName','StateURL']
			data=bsobj.find('div',{'class','list-group'}).findAll('div',{'class':'row'})
			rowexp=re.compile(r'^col-xs-12 prefix-col')
			rowlist=[]
			for row in data:
				rowdict={}
				row=row.findAll('div',{'class':rowexp})
				row=([x.get_text().strip() for x in row])
				rowdata=zip(tableHeaders,row)
				for x in rowdata:
					rowdict[x[0]]=str(x[1])
				rowdict['ZIPURL']=mainurl+rowdict['ZIP Code']
				rowdict['StateName']=stateName
				rowdict['StateURL']=stateURL
				rowlist.append(rowdict)
				print(rowdict)
		return rowlist
		

def requestDelay(*args):
	if args:
		time.sleep(args[0])
	else:
		sleeping_time=random.uniform(2,8)
		# sleeping_time=random.randrange(2,10)
		print('Sleeping for {}'.format(sleeping_time))
		time.sleep(sleeping_time)
				


def getStatesLinks():
	url='https://www.unitedstateszipcodes.org/'
	user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
	headers={'User-Agent':user_agent}
	with requests.get(url,headers=headers) as req:
		if req.status_code == 200:
			bsobj=BeautifulSoup(req.text,'html.parser')
			relativestateslinks=bsobj.find('ul',{'class':'list-unstyled state-links'}).findAll('li')
			stateslinks=['https://www.unitedstateszipcodes.org'+x.find('a').attrs['href'] for x in relativestateslinks]
			statesnames=[x.find('a').get_text() for x in relativestateslinks]
			
			statesdata=zip(statesnames,stateslinks)
			
			statesList=[]
			for state in statesdata:
				statedict={}
				statedict['StateName']=state[0]
				statedict['StateURL']=state[1]
				statesList.append(statedict)
			return statesList

def requestJSONs(url):
	user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
	headers={'User-Agent':user_agent}
	session = renew_connection()
	# session.proxies = {}
	# session.proxies['http'] = 'socks5h://localhost:9050'
	# session.proxies['https'] = 'socks5h://localhost:9050'
	try:
		with session.get(url,headers=headers) as req:
			
			if req.status_code == 200:
				bsobj=BeautifulSoup(req.text,'html.parser')
				script=bsobj.findAll('script')[1]
				# for script in scripts:
				if script.string.strip().startswith('geo'):
					geo=script.string.strip().split(';')[0].lstrip().strip('geojson = ')
					bounds=script.string.strip().split(';')[1].lstrip().strip('bounds = ')
					geoJSON=json.loads(geo)
					boundsJSON=json.loads(bounds)
					# print(geoJSON,boundsJSON)
					return json.dumps(geoJSON,indent=4),json.dumps(boundsJSON,indent=4)
				else:
					print('ZIP code link {} does not has geoJSON data'.format(url))
					return None,None
	except requests.exceptions.ConnectionError as e:
		print(e)
		session=renew_connection()
		# print('Renewed IP={}'.format(newIP))
		main()
	except ConnectionError as e:
		print(e)
		session=renew_connection()
		# print('Renewed IP={}'.format(newIP))
		main()
	except requests.exceptions.SSLError as e:
		session=renew_connection()
		# print('Renewed IP={}'.format(newIP))
		main()


def exportJSONs(url,jsondata):
	filename=url.split('/')[-1]+'.json'
	directory='JSONs'
	if not os.path.exists(directory):
		os.mkdir(directory)
	else:
		pass
	filepath=os.path.join(directory,filename)
	if not os.path.exists(filepath):
		with open(filepath,'w') as jsonfile:
			if jsondata:
				jsonfile.write(jsondata)
			else:
				pass

def checkexistingfiles(directory):
	files=os.listdir(directory)
	fileurls=['https://www.unitedstateszipcodes.org/'+x.split('.')[0]  for x in files]
	# fileurls=[for x in files]
	return fileurls


def main():
	with open('allzipdatawithstate.csv') as data:
		csvreader=csv.DictReader(data)
		zipURLs=[record['ZIPURL'] for record in csvreader]
	directory='JSONs'
	fileurls=checkexistingfiles(directory)
	print('There is already {} files in directory'.format(len(fileurls)))
	print('There are {} urls '.format(len(zipURLs)))
	difference=[url for url in zipURLs if url not in fileurls]
	print('Start scrapping of {} urls'.format(len(difference)))
	counter=0
	randomrequests=random.randint(50,200)
	# randomrequests=5
	print('IP will be changed after {} requests'.format(randomrequests))
	for url in difference:
		print(url)
		counter+=1
		print('Counter={}'.format(counter))
		if counter==randomrequests:
			session=renew_connection()
			# print('IP renewed , new IP is {}'.format(newIP))
			counter=0
			continue
		else:
			try:
				geoJSON,boundsJSON=requestJSONs(url)
				exportJSONs(url,geoJSON)
				print('Exported {} successfully'.format(url))
				# print(requests.get("http://icanhazip.com/").text)
				
				
				# requestDelay(2)
			except TypeError:
				print('ZIP code link {} does not has geoJSON data'.format(url))
				geoJSON=None
				exportJSONs(url,geoJSON)
				pass
		requestDelay()
main()
