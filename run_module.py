from pyairtable import Api
import pandas as pd
from onevizion import Message
import onevizion
import time


def runAndWaitForImport(
		filename, impspec, action, maxRunTimeInMinutes,
		OvUserName, OvPassword, OvUrl):
	tries = -1
	maxTries = maxRunTimeInMinutes * 60 / 10

	Message(filename+' x '+impspec+' x '+action)
	imp = onevizion.Import(
		userName = OvUserName,
		password = OvPassword,
		URL = OvUrl,
		impSpecId=impspec,
		file=filename,
		action=action,
		comments=filename,
		isTokenAuth=True
		)

	if len(imp.errors)>0:
		Message(imp.errors)
		Message('ERROR: Could not run import')
		return False

	PID = imp.processId
	Message(PID)

	d = True
	while d:
		time.sleep(10)
		Message('Checking status '+str(tries)+' tries '+str(tries/6)+' minutes')

		tries = tries + 1
		if tries>maxTries:
			#todo more error handling
			Message('ERROR: Import took too long')
			return False

		# without impSpecId and file it will not attempt to run an import on object creation
		imp = onevizion.Import(
			userName = OvUserName,
			password = OvPassword,
			URL = OvUrl,
			impSpecId=None,
			file=None,
			action=None,
			comments=None,
			isTokenAuth=True
			)
		process_data = imp.getProcessData(processId=PID)

		if len(imp.errors)>0:
			Message(imp.errors)
			Message("ERROR: Could not get Process Data")
			tries = tries + 1
			continue

		if process_data["status"] in ['EXECUTED','EXECUTED_WITHOUT_WARNINGS','EXECUTED_WITH_WARNINGS']:
			d = False

	if process_data["status"] in ['EXECUTED','EXECUTED_WITHOUT_WARNINGS','EXECUTED_WITH_WARNINGS']:
		return True


def run_module(settings):

	api = Api(settings["creds"]["AirtableToken"])

	for tbl in settings["tables"]:
		print(tbl)
		table = api.table(base_id=tbl["base_id"],table_name=tbl["table_name"])
		fields = []
		for f in tbl["fields"]:
			fields.append(f)
		df = pd.DataFrame(dtype=str,columns=fields)
		for row in table.all(formula=tbl["formula"]):
			#print(row)
			a = {}
			for f in tbl["fields"]:
				fieldname = tbl["fields"][f]["fieldname"]
				conditionalfield = tbl["fields"][f]["conditionalfield"]
				conditionalvalue = tbl["fields"][f]["conditionalvalue"]
				print(fieldname,conditionalfield,conditionalvalue)

				if fieldname in row["fields"]:
					if len(conditionalfield)>0:
						if row["fields"][conditionalfield] == conditionalvalue:
							a[f]=row["fields"][fieldname]
						else:
							a[f]=""
					else:
						a[f]=row["fields"][fieldname]
				else:
					a[f]=""
			df.loc[len(df)] = a
		print(df)
		print(tbl["formula"])
		df.to_csv('xx.csv', index=False)
		runAndWaitForImport(
			  filename='xx.csv',
			  impspec=tbl["import"]["impspecid"],
			  action=tbl["import"]["action"],
			  maxRunTimeInMinutes=tbl["import"]["maxruntime"],
			  OvUserName=settings["creds"]["OvAccessKey"],
			  OvPassword=settings["creds"]["OvSecretKey"],
			  OvUrl=settings["creds"]["OvUrl"]
			  )