import pyodbc
import pandas as pd
import sys
import os
import glob
import re
import datetime
import numpy as np
import math
import time
import calendar
import csv
import dateparser as dp
from shutil import move
from shutil import rmtree
from itertools import groupby
from dateutil.parser import *
from collections import Counter
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
from dateutil.relativedelta import relativedelta

con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
					"Server=sql-looker-db.database.windows.net;"
						"Database=Looker_live;" 
						"uid=atg-admin;pwd=Travel@123;")
cursor = con.cursor()
Back_office_agencies_list = pd.read_sql_query('select agency_id, Email_subject from Back_office_agencies_list',con)
# df = pd.DataFrame({"agency_id":0, 
# 				"Email_subject":'For Testing Purpose'}, index=[0])
# Back_office_agencies_list = Back_office_agencies_list.append(df, ignore_index=True)


Back_office_agencies_list = Back_office_agencies_list.append(({"agency_id":0, 
				"Email_subject":'For Testing Purpose'}, index=[0]), ignore_index=True).to_string(index=False) #
# print(Back_office_agencies_list)

# Back_office_agencies = Back_office_agencies_list.to_string(index=False)
# print(Back_office_agencies)

def get_non_negative_int(prompt):
    while True:
        try:
            value = int(input(prompt))
        except ValueError:
            print("Sorry, I didn't understand that.")
            continue
        
		if value.isin(Back_office_agencies_list.fd) and value != 0: print("values is --> ", value)
		email_subject = Back_office_agencies_list.iloc[np.flatnonzero(Back_office_agencies_list['Email_subject'])]
		Yes_No = input(f"Do You want to process {email_subject} ? \n press y for Yes and n for No")
		
        # for i, row in Back_office_agencies_list.iterrows():
        # 	if value  == row['agency_id'] and value != 0:
        # 		print("values is --> ",value)
        # 		if value == row['agency_id']:
        # 			email_subject = row['Email_subject']
        # 			print(f"Do You want to process {email_subject} ? ")
        # 			Yes_No = input("\nIF Yes Press y IF No press n:  ")

        if value not in Back_office_agencies_list['agency_id']:
        #     print("Sorry, your response must not be negative, and must be in agencies ID's list.")
        #     continue
        elif value == 0:
            Yes_No = 'n'
            break
        else:
            break
    return value , Yes_No

value,Yes_No = get_non_negative_int("Enter agency_id: ")

########Connect to database start here########
cnxnLive = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
						"Server=atg-dev.database.windows.net;"
						 "Database=atgdevdb;" 
						 "uid=atg-admin;pwd=dev13579*;")
TcacctsLive = pd.read_sql_query('''SELECT *  FROM globalAccounts_globalaccount ''', cnxnLive)
account_list = TcacctsLive.loc[:,['AIAN_DK','acc_name','acc_number','agency','agency_email','client','company','country','created_at','created_by_id','currency','id','login_id','note','status','updated_at','updated_by_id']]
account_list.columns =["AIAN/DK" , "Account Name" , "Acct Number" , "Agency" , "AgencyEmail" , "Client" , "Company" , "Country" , "created_at" , "created_by_id" , "Currency" , "id" , "LoginID" , "Notes" , "Status" , "updated_at" , "updated_by_id"]

CurrentDir = os.getcwd()
print (CurrentDir)
# MerckDir = '//10.0.0.245/e$/From desktop/MerckForward/MerckData'
# os.chdir(MerckDir)
result = glob.glob('*.csv')
r = re.compile("hrFeed-.*csv")
result = list(filter(r.match, result))
print(result)

# exit()
HRdates=[]
for files in result:
	try:
		match = re.search(r'\d{4}-\d{2}-\d{2}', files)
		date = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()
		HRdates.append(date)
	except:
		pass
# the most updated date
HrLastDate = max(HRdates)
HrLastDate_day = str(HrLastDate.day)
if len(HrLastDate_day) == 1: 	HrLastDate_day= '0'+HrLastDate_day
HrLastDate_month = str(HrLastDate.month)
if len(HrLastDate_month) == 1: 	HrLastDate_month= '0'+HrLastDate_month
FileName = 'hrFeed-'+ str(HrLastDate.year) + '-'+ HrLastDate_month + '-' + HrLastDate_day + '.csv'
# read hr file
hrFeed = pd.read_csv(FileName,encoding='cp1252')
hrFeed.columns =["employee_id" , "login_id" , "first_name" , "last_name" , "email_address" , "active" , "work_country" , "employee_number" , "local_firstname" , "local_lastname" , "cost_center" , "work_phone" , "cell_phone" , "manager_email" , "cmg_number" , "location_ID" , "business_sector" , "division" , "department" , "cmgGJCRoleCode","genInternalEventManager"]
os.chdir(CurrentDir)
print(hrFeed)

hrFeed.to_csv('HRFEED.csv')

# These statements import modules from python's standard library in order to give me access to additional functions
#  I:\ATG Data Scripts\

# These next statements create and populate the dictionaries I use as references in my code.  Most of these pull from the extra CSVs I keep in the IN folder.
# Creates the airport dictionary from the Airport_master file.  The three letter IATA code (Citycode or Metro in the data) is the key, and city name and country are the values, along with STATE for US/CA locations.
# account_list = pd.read_csv('LUT Global Account List.csv')
print("Master Countries Starts :)")

account_list['Acct Number'] = account_list['Acct Number'].apply(lambda x: str(x).strip(' '))
lut_currencies = pd.Series(account_list['Currency'].values,index=account_list['Acct Number']).to_dict()
print(account_list)

lut_cars = pd.read_csv('LUT Car Company Codes.csv')

car_code_to_company = pd.Series(lut_cars['COMPANY'].values,index=lut_cars.CARCODE).to_dict()
car_keyword_to_code = pd.Series(lut_cars['CARCODE'].values,index=lut_cars.Keyword).to_dict()
car_keyword_to_company = pd.Series(lut_cars['COMPANY'].values,index=lut_cars.Keyword).to_dict()
car_keyword_to_cartype = pd.Series(lut_cars['CARTYPE'].values,index=lut_cars.Keyword).to_dict()

lut_hotels = pd.read_csv('LUT Hotel Chain Codes.csv')
hotel_chain_code = pd.Series(lut_hotels['CHAINCOD'].values,index=lut_hotels.CHAINCOD)

account_dict = {}
acctName_dict = {}
account_merck =[]
account_num_merck = []
updated_cars =[]
updated_hotels =[]
updated_trips =[]
updated_legs =[]
updated_services =[]
updated_udids =[]
remove_reascode_acct = []

for i, row in account_list.iterrows():
	account_dict[str(row['Acct Number']).strip()] = [row['Country'],str(row['Client']).upper(),row['Currency']]  
	acctName_dict[str(row['Acct Number']).strip()] = row['Account Name']
	if 'MERCK' in row['Account Name']:
		account_merck.append(row['Account Name'])
		account_num_merck.append(row['Acct Number'])

	if row['LoginID'] == 'ATGDE' or row['LoginID'] == 'ATGEMEA':
		remove_reascode_acct.append(row['Acct Number'])

airport_csv = pd.read_csv('Airport_master.csv',encoding='latin-1')
airport_dict = {}
for i, row in airport_csv.iterrows():
	airport_dict[row['CODE']] = [row['NAME'],row['COUNTRY']]
	if airport_dict[row['CODE']][-1] == 'US' or airport_dict[row['CODE']][-1] == 'CA':
		airport_dict[row['CODE']].insert(1,row['STATE'])

# Here I load the proper header and tablenames from a standard file.  I use this as reference to standardize headers for each table.
headers = pd.read_csv('headers.csv')

# This loads the account information from our master file into a dictionary whose key is the account number, and which contains info on country, client, and currency code.
# It also creates another dictionary, acctName_dict, which standardizes the account names used in TCACCTS by matching them to what we have on file in the master by account number.
# This creates a dictionary used to convert the long form of country names into the appropriate two letter country code.  It is mainly used to clean up the HOTCOUNTRY field in TCHOTEL.	
country_csv = pd.read_csv('countries.csv')
country_dict = {}
for i, row in country_csv.iterrows():
	country_dict[str(row['Name']).upper()] = row['Code']

currency_csv = pd.read_csv('EQX Country Curency Codes.csv',encoding='latin-1')
currencies = list(currency_csv['CurrCd'])

currency_dict = {}
for i, row in currency_csv.iterrows():
	currency_dict[str(row['CntryCd']).upper()] = row['CurrCd']

# This creates a master dictionary, d, that holds all of the data from all of the directories the script reads.  Each item in the dictionary is a DataFrame object with the key of the appropriate table name.	
d = {}
for i in headers.columns:
	d[i] = pd.DataFrame() ###???

# Here are the dictionaries I use to map a two number code for country and a two number code for client to each reckey.  I have them hardcoded to ensure that the same code is used each time.
country_number_dict = {}
_country_number_dict = {}

reckeyStart = 99
ccodes = pd.read_csv('LUT Country Codes.csv',na_filter = False)

for i,row in ccodes.iterrows():
	if str(row['CntryCd2']) != 'nan' and str(row['CntryCd2']) != '':
		reckeyStart = reckeyStart+1 
		country_number_dict[row['CntryCd2']] = reckeyStart
		_country_number_dict[reckeyStart] = row['CntryCd2']
		
# clients_dict = {'RED HAT': '08', 'NIKE': '06', 'GREIF': '03', 'ZF': '11', 'ESCO': '02', 'MCDONALDS': '04', 'MERCK': '05', 'SKECHERS': '09', 'TOLL': '10', 'PARADIGM': '07', 'AVNET': '01','ATG': '12','TUPPERWARE': '13','JAIRBV':'14','KLEY':'15','WELBILT':'16','WH':'17','PRIMA':'18','PROPHET':'19','CERTIS':'20','HILTI':'21','DEKKER':'22','SERCO':'23'}
# clients_dict = {'RED HAT': '08', 'NIKE': '06', 'GREIF': '03', 'ZF': '11', 'ESCO': '02', 'MCDONALDS': '04', 'MERCK': '05', 'SKECHERS': '09', 'TOLL': '10', 'PARADIGM': '07', 'AVNET': '01','ATG': '12','TUPPERWARE': '13','JAIRBV':'14','KLEY':'15','WELBILT':'16','WH':'17','PRIMA':'18','PROPHET':'19','CERTIS':'20','HILTI':'21','DEKKER':'22','SERCO':'23','RIVERLAND':'24','COCOON':'25','SCAPA' : '26' ,'DEKKER CHRYSANTEN BV' : '27' ,'DDM Demontage BV' : '28' ,'DDM Belgium' : '29' ,'DDM Deutschland GmbH' : '30' ,'Tupperware Brands' : '31' ,'TEST' : '32','Vianen KVS BV':'33','Merck Chile':'33','Scapa UK':'34','wargaming':'35','kaercher de':'36', 'jos de vries retail company bv' :'37', 'zf group' : '11', 'varian': '38','kaercher':'39', 'zf de': '11', 'zf at': '11', 'zf nl':'11', 'zf srb': '11', 'zf es': '11', 'zf fr': '11', 'zf pl':'11', 'zf ru':'11','zf tk':'11', 'zf pt': '11','zf dk': '11','zf be':'11','zf sk': '11','ZF UK': '11'}
# clients_dict = {'RED HAT': '08', 'NIKE': '06', 'GREIF': '03', 'ZF': '11', 'ESCO': '02', 'MCDONALDS': '04', 'MERCK': '05', 'SKECHERS': '09', 'TOLL': '10', 'PARADIGM': '07', 'AVNET': '01','ATG': '12','TUPPERWARE': '13','JAIRBV':'14','KLEY':'15','WELBILT':'16','WH':'17','PRIMA':'18','PROPHET':'19','CERTIS':'20','HILTI':'21','DEKKER':'22','SERCO':'23','RIVERLAND':'24','COCOON':'25','SCAPA' : '26' ,'DEKKER CHRYSANTEN BV' : '27' ,'DDM Demontage BV' : '28' ,'DDM Belgium' : '29' ,'DDM Deutschland GmbH' : '30' ,'Tupperware Brands' : '31' ,'TEST' : '32','Vianen KVS BV':'33','Merck Chile':'33','Scapa UK':'34','wargaming':'35','kaercher de':'36', 'jos de vries retail company bv' :'37', 'zf group' : '11', 'varian': '38','kaercher':'39', 'ZF UK': '40', 'smd': '41', 'zf de': '42', 'ZF SRB':'43','zf nl':'44', 'zf it': '45', 'zf be': '45', 'zf at':'46', 'zf sk':'47', 'zf fr': '48', 'zf pl': '49', 'vw ch': '50', 'belen garijo': '51','allergopharma de':'52','vw kr':'53', 'l - founders of loyalty co\xc3\xb6peratie u.a.': '54', 'l - founders of loyalty western europe b.v.': '55', 'l - founders of loyalty group b.v.': '56', 'l - founders of loyalty sourcing b.v.':'57','vw id': '58'}
clients_dict = {'RED HAT': '08', 'NIKE': '06', 'GREIF': '03', 'ZF': '11', 'ESCO': '02', 'MCDONALDS': '04', 'MERCK': '05', 'SKECHERS': '09', 'TOLL': '10', 'PARADIGM': '07', 'AVNET': '01','ATG': '12','TUPPERWARE': '13','JAIRBV':'14','KLEY':'15','WELBILT':'16','WH':'17','PRIMA':'18','PROPHET':'19','CERTIS':'20','HILTI':'21','DEKKER':'22','SERCO':'23','RIVERLAND':'24','COCOON':'25','SCAPA' : '26' ,'DEKKER CHRYSANTEN BV' : '27' ,'DDM Demontage BV' : '28' ,'DDM Belgium' : '29' ,'DDM Deutschland GmbH' : '30' ,'Tupperware Brands' : '31' ,'TEST' : '32','Vianen KVS BV':'33','Merck Chile':'33','Scapa UK':'34','wargaming':'35','kaercher de':'36', 'jos de vries retail company bv' :'37', 'zf group' : '11', 'varian': '38','kaercher':'39', 'ZF UK': '40', 'smd': '41', 'zf de': '42', 'ZF SRB':'43','zf nl':'44', 'zf it': '45', 'zf be': '45', 'zf at':'46', 'zf sk':'47', 'zf fr': '48', 'zf pl': '49', 'vw ch': '50', 'belen garijo': '51','allergopharma de':'52','vw kr':'53', 'l - founders of loyalty co\xc3\xb6peratie u.a.': '54', 'l - founders of loyalty western europe b.v.': '55', 'l - founders of loyalty group b.v.': '56', 'l - founders of loyalty sourcing b.v.':'57','vw':'58','merck vip private':'59' , 'kley private':'60'}
# clients_dict = {'RED HAT': '08', 'NIKE': '06', 'GREIF': '03', 'ZF': '11', 'ESCO': '02', 'MCDONALDS': '04', 'MERCK': '05', 'SKECHERS': '09', 'TOLL': '10', 'PARADIGM': '07', 'AVNET': '01','ATG': '12','TUPPERWARE': '13','JAIRBV':'14','KLEY':'15','WELBILT':'16','WH':'17','PRIMA':'18','PROPHET':'19','CERTIS':'20','HILTI':'21','DEKKER':'22','SERCO':'23','RIVERLAND':'24','COCOON':'25','SCAPA' : '26' ,'DEKKER CHRYSANTEN BV' : '27' ,'DDM Demontage BV' : '28' ,'DDM Belgium' : '29' ,'DDM Deutschland GmbH' : '30' ,'Tupperware Brands' : '31' ,'TEST' : '32','Vianen KVS BV':'33','Merck Chile':'33','Scapa UK':'34','wargaming':'35','kaercher de':'36', 'jos de vries retail company bv' :'37', 'zf group' : '11', 'varian': '38','kaercher':'39', 'zf it':'45'}
# print country_number_dict
# exit()
# The last dictionary here is used to map some of the station names used in TCLEGS['ORIGIN'] and TCLEGS['DESTINAT'] to the appropriate three letter code.
	
stations = pd.read_csv('Stations.csv')
station_dict = {}
for i, row in stations.iterrows():
	station_dict[row[0]] = row[1]

# print station_dict
# exit()
################################################################################################################
def validateReascode(x):
	reasCodes = ['AL','AA','AB','AC','AD','AE','AF','AH','AN','AP','AQ','AR','AT','AU','AW','AX','AZ']
	if x.find('/') != -1 or x.find('#') != -1:
		if x[:2] in reasCodes:
			return x[:2]
	if x in reasCodes :
		return x
	return ''

def fix_format(x):

	sfp = x.find('/')
	fp = x[:sfp]
	ssp = x.rfind('/')
	sp = x[sfp+1:ssp]
	tp = x[ssp+1:len(x)]
	return str(sp)+'/'+str(tp)+'/'+str(fp)

def remInvalidDates(x):
	
	if x == '00/00/0000' or x == '0001-01-01':
		return ''
	return x

def fix_invalid_date(date):
	# regex = re.compile('[^0-9-/]')
	# date = regex.sub('', date)

	print('date', str(date))
	if len(date) > 0 and str(date) != 'nan' and str(date) != 'NAT':
		searchFirstPart = date.find('/')
		dateFirstPart = date[0:searchFirstPart]
		dateFirstPart = '0'+str(dateFirstPart) if len(dateFirstPart) < 2 else str(dateFirstPart)

		searchSecondPart = re.search('(/)(.*?)(/)',date)
		dateSecondPart = searchSecondPart.group(2)
		dateSecondPart = '0'+str(dateSecondPart) if len(dateSecondPart) < 2 else str(dateSecondPart)

		searchThirdPart = date.rfind('/')
		dateThirdPart = date[searchThirdPart+1:]
		dateThirdPart = '20'+str(dateThirdPart) if len(dateThirdPart) < 4 else str(dateThirdPart)
		

		return str(dateFirstPart) + '/' + str(dateSecondPart) + '/' + str(dateThirdPart)
	return ''
############## Not Using  ###############
def fix_date_format(date,month):
	if len(date) > 0:
		
		date_day_s = date.find('/')
		date_day = date[:date_day_s]
		

		date_month_s = date.rfind('/')
		date_month = date[date_day_s+1:date_month_s]

		year = date[-4:]
		# if date_day != '10' :
		# 	date = date_month +'/'+ date_day +'/'+ year		
		return date
	return ''

##########################################	
def convert_date(x):
	return x

def dateWithTripleForwardSlash(x,month):
	# print x
	# x => date
	countChar = dict((letter,x.count(letter)) for letter in set(x))

	if "/" in countChar and countChar["/"] == 3:
		year = x[0:x.find("/")]
		date = x[x.rfind("/")+1:len(x)]
		return str(month) + "/" + str(date) + "/" + str(year)
	return x

def fix_time(time):
	if len(time) > 0 and time[-1] == ':':
		return time + '00'
	return ''

def is_number(s):
	try:
		float(s)
		return True
	except ValueError:
		pass
	try:
		import unicodedata
		unicodedata.numeric(s)
		return True
	except (TypeError, ValueError):
		pass
		return False
		
# [fs] i added bellow function to reenforece the code
def StartWithDay(string):
	"""
	return if date start with day first
	"""
	try:
		x= string.split('/') # [13,08,2019]
		x2 = [int(i) for i in x]
		if x2[0]> 12 and x2[1] < 13:
			return True
		else:
			return False
	except:
		return False

# The batch_script function contains the actual code for the script, as opposed to the data sources or modules defined or called above.

def master_script():
	start_time = time.time()
	# Creates a string with today's date, in the format described below.  Used to name the folder the data is written into.

	date = time.strftime("%m_%d_%Y")
	# date = 'data'
	# This error report array is used to contain records of any errors found and annotated by the script.  It is used to populate the error_report csv.

	error_report = []

	# This array will store the name of every directory read by the script.  It is used to populate the directories csv.

	directories = []
	_flag = 0
	# This code creates a list of every folder in the directory, excluding hidden ones that contain files used by the system as opposed to the users.

	# folders = filter( lambda f: not f.startswith('.'), os.walk('.').next()[1])
	# folders = filter(lambda f: not f.startwith('.'), next(os.walk('.'))[1])

	# folders = [d for d in os.listdir(os.getcwd()) if os.path.isdir(d) & (~d.startswith('.'))]

	folders = [d for d in os.listdir(CurrentDir) if os.path.isdir(d) & (~d.startswith('.'))]
	print(folders)
	# This next block of code iterates over each folder, processing the contents and loading them into the overall dictionary 'd' defined above, then deleting the folder.
	# It is important that the only folders in the IN directory when the script is run contain 7 files and only 7 files.
	# The file extensions aren't that important, but the names cannot include anything but letters, numbers, and parentheses.  If they do, the script will kick up an error.
	# Some clients, like France Nike, can give file names that need to be changed before they can be run.  Look out for this.

	# [FS] when generating reckey this tripNumber should be above the loop so we dont have conflit 
	# when we merge to files
	tripNumber = 1
	# [FS] when generating reckey this tripNumber should be above the loop so we dont have conflit 
	# when we merge to files
	for folder in folders:
		# 'h' is a dictionary created again for each folder that stores that folder's data.
		# You can think of it as the temp dictionary to 'd's permanent one.  'd' holds all the data from all the folders, while 'h' only holds one folder's data.
	
		h = {}

		# The following for-loop iterates over each file in the folder.  You'll note by the spacing that is contained within the for-loop for each folder.
		# That means that for each folder in the IN directory, it iterates over each file in each folder individually.
		# glob.glob grabs each file in the path specified, the format(folder) code adds the name of the folder to the path, and the code grabs each file with .csv, .txt, .CSV, and .TXT extensions.

		for j in glob.glob("{}\*.csv".format(folder)) + glob.glob("{}\*.txt".format(folder)) + glob.glob("{}\*.CSV".format(folder)) + glob.glob("{}\*.TXT".format(folder)):
			# This code splits the file name on the '\' character, separating out the folder name from the filename.  It produces an array called split.

			split = j.split('\\')
			
			# These two lines grab the first element in the split array, which will be the folder name, capitalize it, and then add it to the directories array.
			# It is important that directories be defined outside the inner loop, but inside the outer loop.  This allows the directories array to contain every folder used.
			# If it were defined within the inner loop, you would end up with only the last folder read, as it would overwrite it each time.
			directory = split[0].upper()
			directories.append(directory)

			# The next few lines take the last element in the split array, which is the file name, and processes it.
			# Processing it involves stripping out the file extension, capitalizing it, removing spaces and numbers and parentheses, and correcting for a few common incorrect names.
			# It is important that each filename matches a table name (TCACCTS,TCUDIDS,etc.), otherwise the code will kick up an error.
			
			tableName = split[-1].replace('.txt', '').replace('.Txt','').replace('.csv', '').replace('.CSV', '').replace('.TXT', '').upper().replace('(','').replace(')','').strip()
			tableName = ''.join([i for i in tableName if not i.isdigit()])
			tableName = tableName.strip()
			if tableName == 'TCHOTELS':
				tableName = 'TCHOTEL'
			if tableName == 'TCLEG':
				tableName = 'TCLEGS'
			if tableName == 'TCACCOUNTS':
				tableName = 'TCACCTS'
			if tableName != 'ERROR_REPORT':
				# This block of code actually reads each file and loads it into the temporary dictionary 'h'.
				# with open(j,'rU') as f:
				with open(j,'r') as f:

					# Opens up the object that actually reads the csv, called reader.
					# Creates an element called columns, which is the data from the first line of the csv.
					reader = csv.reader(f)
					columns = next(reader,None)
					
					# Checks to make sure the columns variable is defined and isn't empty.
					# print tableName
					if columns != None and len(columns) != 0:
						
						# Creates a variable called first that consists of the capitalized version of the first field of the first line of the csv.
						# It also corrects for some of the common variations that occur in the extracts as to field names.

						first = columns[0].upper().strip()
						if first == 'RECORDKEY':
							first = "RECKEY"
						if first == 'RECORDNO':
							first = "RECKEY"
						if first == 'ACCOUNT NUMBER':
							first = "ACCT"

						# This code uses the headers file defined at the top, whose column headers are the name for each table.
						# If the tablename has the correct column names for the appropriate table, it triggers this code.
						# This code is why it is important that tablesnames be standardized, as they were above, and that the folders only contain
						# 7 files.  Otherwise, this code will kick up a key error.

						if headers[tableName][0] == first:
							# Even though the first variable is what it is supposed to be, the files headers will be replaced anyways to ensure
							# that all files are uniform.  The 'null' values in the headers file, which were added to make the file 'square',
							# are also removed at this time.
							columns = headers[tableName]
							columns = [i for i in columns if i != 'null']
							columns = [i for i in columns if str(i) != 'nan']
							# Some clients send TCACCTS files with extra columns, and so files with the tableName 'TCACCTS' only have the first
							# two columns read.  For various reasons to do with how the extracts were originally encoded, and which to be honest 
							# I don't entirely understand, the try/except blocks are necessary, as one of the two engines will work to read the files
							# properly.  The files headers are replaced with the column variable defined above, the first line (containing
							# the original file headers) is skipped, and the index column is ignored.
							
							if tableName == 'TCACCTS':
								print (headers)
								exit()
								try:
									df = pd.read_csv(j,names=columns,header=0,usecols=columns, index_col=False)
									# df = pd.read_csv(j,names=columns,header=None,index_col=False)
									# df = pd.read_csv(j,names=columns,skiprows=1,index_col=False,usecols=[0,1])
								except:
									df = pd.read_csv(f,names=columns,engine='python',header=0,usecols=columns,index_col=False)
									# df = pd.read_csv(f,names=columns,engine='python',header=None,index_col=False)
									# df = pd.read_csv(f,names=columns,engine='python',skiprows=1,index_col=False,usecols=[0,1])
							else:
								try:
									df = pd.read_csv(j,names=columns,header=None,usecols=columns,index_col=False,low_memory=False)
								except:
									df = pd.read_csv(f,names=columns,engine='python',header=None,usecols=columns,index_col=False)

							# Once the file is read into a DataFrame, the resulting DataFrame is written into the temp dictionary.
							h[tableName] = df
						else:
							# If the variable 'first' does not match, then the file is assumed to have no headers.
							# The following code does the same thing as the above, except that it doesn't skip the top row,
							# as this would involve removing the first row of actual data rather than the old headers.
							columns = headers[tableName]
							columns = [i for i in columns if i != 'null']
							columns = [i for i in columns if str(i) != 'nan']
							if tableName == 'TCACCTS':
								try:
									df = pd.read_csv(j,names=columns,header=None,usecols=columns,index_col=False)
									# df = pd.read_csv(j,names=columns,header=None,index_col=False,usecols=[0,1])

								except:
									df = pd.read_csv(f,names=columns,engine='python',header=None,usecols=columns,index_col=False)
									# df = pd.read_csv(f,names=columns,engine='python',header=None,index_col=False,usecols=[0,1])

							else:
								try:
									df = pd.read_csv(j,names=columns,header=None,index_col=False,low_memory=False)
								except:
									df = pd.read_csv(f,names=columns,engine='python',header=None,index_col=False)
							#print "%s/%s: Complete." % (directory,tableName)
							h[tableName] = df
					else:
						# This code is for files with proper names (TCTRIPS, TCLEGS, etc.) but no data, which we get periodically.
						# This code block adds the appropriate columns so that the rest of the script runs without errors.
						columns = headers[tableName]
						columns = [i for i in columns if i != 'null']
						columns = [i for i in columns if str(i) != 'nan']
						#print "%s/%s: Starting." % (directory,tableName)
						df = pd.DataFrame(columns=columns)
						#print "%s/%s: Complete." % (directory,tableName)

						h[tableName] = df

		
		# This block of code is outside the inner loop, but inside the outer loop.  This means that it is working on each folder's worth of data.
		# What it does is loop through the temp dictionary 'h', changing the reckey values and separating out entries in TCHOTEL, TCLEGS, TCCAR,
		# and the like that don't have corresponding records in TCTRIPS.  It then adds the temp data to the overall dictionary 'd', and then 
		# deletes the folder.
		# print h[tableName]
		# If len(h) != 0 is to prevent errors from being returned from an empty directory.  This code matters for the version of this script
		# that runs off of my computer.
		def parseDate(date):
			# print("DATE =", date)
			# exit()
			# if date == 'nan':
			# 	return date
			print('date_1', date)
			date = dp.parse(date)
			print('date_res', date)
			year = str(date)[:4]
			month = str(date)[5:7]
			day = str(date)[8:10]
			date = month + '/' + day + '/' + year
			return date
		# print h
		# print h['TCTRIPS']
		# exit()
		# exit()
		print (h['TCTRIPS'])
		# exit()
		h['TCTRIPS']['RECKEY'] = h['TCTRIPS']['RECKEY'].apply(lambda x: str(x).lstrip('0'))
		h['TCHOTEL']['RECKEY'] = h['TCHOTEL']['RECKEY'].apply(lambda x: str(x).lstrip('0'))
		h['TCLEGS']['RECKEY'] = h['TCLEGS']['RECKEY'].apply(lambda x: str(x).lstrip('0'))
		# h['TCSERVICES']['RECKEY'] = h['TCSERVICES']['RECKEY'].apply(lambda x: str(x).lstrip('0'))
		h['TCCARS']['RECKEY'] = h['TCCARS']['RECKEY'].apply(lambda x: str(x).lstrip('0'))
		h['TCUDIDS']['RECKEY'] = h['TCUDIDS']['RECKEY'].apply(lambda x: str(x).lstrip('0'))
		# print(h['TCLEGS'])
		# print(h['TCLEGS']['RECKEY'])
		# exit()
		# print "Hello WElCoMEeeee"
		# exit()
		# try:
		# 	h['TCTRIPS']['INVDATE'] = h['TCTRIPS']['INVDATE'].apply(lambda x: parseDate(x))
		# except:
		# 	pass
		# h['TCTRIPS']['DEPDATE'] = h['TCTRIPS']['DEPDATE'].apply(lambda x: dp.parse(x))
		# h['TCTRIPS']['ARRDATE'] = h['TCTRIPS']['ARRDATE'].apply(lambda x: dp.parse(x))
		# h['TCLEGS']['RARRDATE'] = h['TCLEGS']['RARRDATE'].apply(lambda x: dp.parse(x))
		# h['TCLEGS']['RDEPDATE'] = h['TCLEGS']['RDEPDATE'].apply(lambda x: dp.parse(x))
		# exit()
		print("You have crossed 500 lines :)")
		# print("Hello ",h)
		# exit()
		# for i,row in h['TCTRIPS'].iterrows():
		# 	print(row['INVDATE'])
		# 	year = str(row['INVDATE'])[:4]
		# 	day = str(row['INVDATE'])[5:7]
		# 	month = str(row['INVDATE'])[8:10]
		# 	h['TCTRIPS'].loc[i,'INVDATE'] = month +'/'+ day +'/'+ year

		# for i,row in h['TCTRIPS'].iterrows():
		# 	year = str(row['DEPDATE'])[:4]
		# 	month = str(row['DEPDATE'])[5:7]
		# 	day = str(row['DEPDATE'])[8:10]
		# 	h['TCTRIPS'].loc[i,'DEPDATE'] = month +'/'+ day +'/'+ year

		# for i,row in h['TCTRIPS'].iterrows():
		# 	year = str(row['ARRDATE'])[:4]
		# 	month = str(row['ARRDATE'])[5:7]
		# 	day = str(row['ARRDATE'])[8:10]
		# 	h['TCTRIPS'].loc[i,'ARRDATE'] = month +'/'+ day +'/'+ year
		
		# for i,row in h['TCLEGS'].iterrows():
		# 	year = str(row['RARRDATE'])[:4]
		# 	month = str(row['RARRDATE'])[5:7]
		# 	day = str(row['RARRDATE'])[8:10]
		# 	h['TCLEGS'].loc[i,'RARRDATE'] = month +'/'+ day +'/'+ year

		# for i,row in h['TCLEGS'].iterrows():
		# 	year = str(row['RDEPDATE'])[:4]
		# 	month = str(row['RDEPDATE'])[5:7]
		# 	day = str(row['RDEPDATE'])[8:10]
		# 	h['TCLEGS'].loc[i,'RDEPDATE'] = month +'/'+ day +'/'+ year

		# print(h['TCUDIDS']['UDIDNO'].unique(),'1111111111111111111111111111')
		# print('---------------------------------------------------------------------------------------')

		# h['TCTRIPS']['RECKEY'] = h['TCTRIPS']['RECKEY'].apply(lambda x: int(x))
		count = h['TCTRIPS']['RECKEY'].count()
		# count = h['TCLEGS']['RECKEY'].count()
		# print(count)
		# exit()
		try:
			bkSum =h['TCTRIPS']['BKTOOL'].isnull().sum()
			if count == bkSum:
				h['TCTRIPS'].drop('BKTOOL', axis=1, inplace=True)
		except:
			pass
		

		if len(h) != 0:

			# _flag = h['TCACCTS']['ACCT'].apply(lambda x: )

			for i, row in h['TCACCTS'].iterrows():
				ReascodeRuleCheckList = ['MKMY','MKPH','MKSG','MKMY','MKTH']
				if str(row['ACCT'])[:4] in ReascodeRuleCheckList:
					_flag = 1

			if len(h['TCTRIPS']) != 0:
				# Creates the reference dictionary that maps the old reckey values to the new.

				reckey_converter = {}

				# Selects the trips data from the temp dict.

				trips = h['TCTRIPS']

				# Cleans the trips data by capitalizing everything, removing excess spaces, commas, and 'NAN' values.

				# Rule after finding nan in autocity in cars.csv
				# trips = trips.applymap(lambda x: str(x).upper().strip().replace(',','').replace('NAN',''))
				trips = trips.applymap(lambda x:str(x).upper().strip().replace(',','').replace('NAN','') if x == "" or x=="nan" else str(x).upper().strip().replace(',',''))

				# fix ATPI FRANCE Accounts inside trips
				acct_to_fix = {"180882":"882","180883":"883","180884":"884","180885":"885","182458":"2458"}
				print("STEP 1:",trips['ACCT'])
				trips['ACCT'] = trips['ACCT'].apply(lambda x: acct_to_fix[str(x)] if str(x) in acct_to_fix else x)
				# print "Hello you are here"
				# exit()
				# This for-loop checks if the account listed is in our master, and records an error ('ACCOUNT NOT IN RECORDS') if it is not.
				# It then creates the new reckey values by adding the appropriate country and client values to the front.
				# This new reckey value is then stored in the reckey_converter dictionary using the old reckey as the key.
				# pprint.pprint(account_dict)
				# exit()
				# print("CouNtRy DiCt=",country_number_dict)
				# print("ClIeNt DiCT=",clients_dict)
				# exit()
				print("STEP 2:",trips['ACCT'])

				
				trips['ACCT'] = trips['ACCT'].apply(lambda x: x.lstrip('0'))
				print(trips['ACCT'])
				# exit()
				# [fs] this should be outside folder loop otherwise will cause lot of problem when there are tow files or more
				# tripNumber = 1
				# [fs] this should be outside folder loop otherwise will cause lot of problem when there are tow files or more
				for i, row in trips.iterrows():
					# print("account_dict =",account_dict[row['ACCT']])
					if str(row['ACCT']).replace('.0','').strip().lstrip('0') in account_dict:						
						country = account_dict[str(row['ACCT']).replace('.0','').lstrip('0')][0]
						client = account_dict[str(row['ACCT']).replace('.0','').lstrip('0')][1]
						# pprint.pprint(country_number_dict)
						# print "clients_dict clients_dict ="
						# pprint.pprint(clients_dict)
						# print(account_dict)
						print("IF CoUntry = ",country)
						print("IF CLiEnT = ",client)
						# exit()
						print(client,country)
						# [fs] i add bellow line in except to fix clients_dict is casesensetive
						try:
							reckey_converter[str(row['RECKEY']).replace('.0','').replace('.','')] = str(country_number_dict[country]) + clients_dict[client] + str(tripNumber)
						except:
							clients_dict_lowercase = {k.lower(): v for k, v in clients_dict.items()}
							reckey_converter[str(row['RECKEY']).replace('.0', '').replace('.', '')] = str(country_number_dict[country]) + clients_dict_lowercase[client.lower()] + str(tripNumber)
						tripNumber = tripNumber + 1
						# print(reckey_converter)
						# exit()
					else:
						print("ELSE CoUntry = ")
						print("ELSE CLiEnT = ")
						reckey = str(row['RECKEY'])
						if not reckey.isdigit():
							reckey = list(str(reckey).split(';'))[0]
							print("HeLlo ReCkeY=",reckey)
						error_report.append([directory,'TCTRIPS',reckey[4::],'ACCOUNT NOT IN RECORDS'])
				
    			# print trips['RECKEY']
				# exit()
				# This for-loop goes and replaces the old reckey values with the new ones in the reckey_converter dict.
				# If the reckey does not exist in the dict (which has all of TCTRIPS' old reckeys as keys) then it
				# records an error ('RECKEY NOT IN TRIPS').

				# print(h['TCUDIDS']['UDIDNO'].unique(),'22222222222222222222222222222')
				# print('---------------------------------------------------------------------------------------')
				# print(reckey_converter)
				# print('---------------------------------------------------------------------------------------')
				# print(h['TCTRIPS'])
				# print(reckey_converter)
				# exit()
				# [fs] fix when data has headers
				for name, df in h.items():
					if name != 'TCACCTS' and df.shape[0] >0:
						if df['RECKEY'][0]=='RECKEY':
							h[name] = h[name].drop(0).reset_index(drop=True)
					elif name == 'TCACCTS' :
						if df['ACCT'][0]=='ACCT':
							h[name] = h[name].drop(0).reset_index(drop=True)
				# [fs] fix when data has headers
				for name, df in h.items():
					if name != 'TCACCTS':
						df['RECKEY'] = df['RECKEY'].apply(lambda x: reckey_converter[str(x)] if str(x) in reckey_converter else 0)
						h[name] = h[name][h[name]['RECKEY'] != 0]
						# print("name, df in h ==",h[name])
						# exit()
				
    			# exit()
				# print(h['TCUDIDS']['UDIDNO'].unique(),'33333333333333333333333333333')
				# print('---------------------------------------------------------------------------------------')
				# This last for-loop takes a copy of the data already in the main dict 'd' for that tables, adds the data from here to it,
				# Then writes the whole thing back to the dict.  In this way the data doesn't get overwritten from folder to folder, and you end
				# up with a dictionary that has every folder's data in it, divided up into standardized tables (TCTRIPS,TCLEGS,etc.)
				# print df
				for name, df in h.items():
					store = d[name]
					d[name] = store.append(df,ignore_index=True)
				# print "Hello Dddd",d
				# exit()
		# count = h['TCLEGS']['RECKEY'].count()
		# print(count)
		# exit()
		# fl = open("reckey_converter.txt", "w")
		# fl.write(str(reckey_converter))
		# fl.close()
		
		# print(h['TCTRIPS'].INVDATE)
		h['TCTRIPS'].INVDATE = pd.to_datetime(h['TCTRIPS'].INVDATE)
		h['TCTRIPS'].INVDATE = h['TCTRIPS'].INVDATE.dt.strftime("%m/%d/%Y")
		h['TCTRIPS'].BOOKDATE = pd.to_datetime(h['TCTRIPS'].BOOKDATE)
		h['TCTRIPS'].BOOKDATE = h['TCTRIPS'].BOOKDATE.dt.strftime("%m/%d/%Y")
		h['TCTRIPS'].DEPDATE = pd.to_datetime(h['TCTRIPS'].DEPDATE)
		h['TCTRIPS'].DEPDATE = h['TCTRIPS'].DEPDATE.dt.strftime("%m/%d/%Y")
		h['TCTRIPS'].ARRDATE = pd.to_datetime(h['TCTRIPS'].ARRDATE)
		h['TCTRIPS'].ARRDATE = h['TCTRIPS'].ARRDATE.dt.strftime("%m/%d/%Y")
		h['TCCARS'].DATEBACK = pd.to_datetime(h['TCCARS'].DATEBACK)
		h['TCCARS'].DATEBACK = h['TCCARS'].DATEBACK.dt.strftime("%m/%d/%Y")
		h['TCCARS'].RENTDATE = pd.to_datetime(h['TCCARS'].RENTDATE)
		h['TCCARS'].RENTDATE = h['TCCARS'].RENTDATE.dt.strftime("%m/%d/%Y")
		h['TCHOTEL'].DATEIN = pd.to_datetime(h['TCHOTEL'].DATEIN)
		h['TCHOTEL'].DATEIN = h['TCHOTEL'].DATEIN.dt.strftime("%m/%d/%Y")
		h['TCHOTEL'].DATEOUT = pd.to_datetime(h['TCHOTEL'].DATEOUT)
		h['TCHOTEL'].DATEOUT = h['TCHOTEL'].DATEOUT.dt.strftime("%m/%d/%Y")
		h['TCLEGS'].RDEPDATE = pd.to_datetime(h['TCLEGS'].RDEPDATE)
		h['TCLEGS'].RDEPDATE = h['TCLEGS'].RDEPDATE.dt.strftime("%m/%d/%Y")
		h['TCLEGS'].RARRDATE = pd.to_datetime(h['TCLEGS'].RARRDATE)
		h['TCLEGS'].RARRDATE = h['TCLEGS'].RARRDATE.dt.strftime("%m/%d/%Y")
		
		d['TCTRIPS'].INVDATE = pd.to_datetime(d['TCTRIPS'].INVDATE)
		d['TCTRIPS'].INVDATE = d['TCTRIPS'].INVDATE.dt.strftime("%m/%d/%Y")
		d['TCTRIPS'].BOOKDATE = pd.to_datetime(d['TCTRIPS'].BOOKDATE)
		d['TCTRIPS'].BOOKDATE = d['TCTRIPS'].BOOKDATE.dt.strftime("%m/%d/%Y")
		d['TCTRIPS'].DEPDATE = pd.to_datetime(d['TCTRIPS'].DEPDATE)
		d['TCTRIPS'].DEPDATE = d['TCTRIPS'].DEPDATE.dt.strftime("%m/%d/%Y")
		d['TCTRIPS'].ARRDATE = pd.to_datetime(d['TCTRIPS'].ARRDATE)
		d['TCTRIPS'].ARRDATE = d['TCTRIPS'].ARRDATE.dt.strftime("%m/%d/%Y")
		d['TCCARS'].DATEBACK = pd.to_datetime(d['TCCARS'].DATEBACK)
		d['TCCARS'].DATEBACK = d['TCCARS'].DATEBACK.dt.strftime("%m/%d/%Y")
		d['TCCARS'].RENTDATE = pd.to_datetime(d['TCCARS'].RENTDATE)
		d['TCCARS'].RENTDATE = d['TCCARS'].RENTDATE.dt.strftime("%m/%d/%Y")
		d['TCHOTEL'].DATEIN = pd.to_datetime(d['TCHOTEL'].DATEIN)
		d['TCHOTEL'].DATEIN = d['TCHOTEL'].DATEIN.dt.strftime("%m/%d/%Y")
		d['TCHOTEL'].DATEOUT = pd.to_datetime(d['TCHOTEL'].DATEOUT)
		d['TCHOTEL'].DATEOUT = d['TCHOTEL'].DATEOUT.dt.strftime("%m/%d/%Y")
		d['TCLEGS'].RDEPDATE = pd.to_datetime(d['TCLEGS'].RDEPDATE)
		d['TCLEGS'].RDEPDATE = d['TCLEGS'].RDEPDATE.dt.strftime("%m/%d/%Y")
		d['TCLEGS'].RARRDATE = pd.to_datetime(d['TCLEGS'].RARRDATE)
		d['TCLEGS'].RARRDATE = d['TCLEGS'].RARRDATE.dt.strftime("%m/%d/%Y")
		
		# ['TCSERVICES'].TRANDATE = pd.to_datetime(['TCSERVICES'].TRANDATE)
		# ['TCSERVICES'].TRANDATE = ['TCSERVICES'].TRANDATE.dt.strftime("%m/%d/%Y")
		print(d['TCLEGS'].RARRDATE)
		StartWithDayCount = 0
		INVDATEtartWithDay = sum(h['TCTRIPS'].INVDATE.apply(lambda x: StartWithDay(x)))
		# if INVDATEtartWithDay > 0:
		# 	print('TCTRIPS INVDATEtartWithDay')
		# 	h['TCTRIPS'].INVDATE = pd.to_datetime(h['TCTRIPS'].INVDATE, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCTRIPS'].INVDATE = h['TCTRIPS'].INVDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	h['TCTRIPS'].INVDATE = pd.to_datetime(h['TCTRIPS'].INVDATE, dayfirst=False, format='%m/%d/%Y')
		# 	h['TCTRIPS'].INVDATE = h['TCTRIPS'].INVDATE.dt.strftime('%m/%d/%Y')

		# BOOKDATEStartWithDay = sum(h['TCTRIPS'].BOOKDATE.apply(lambda x: StartWithDay(x)))
		# print(BOOKDATEStartWithDay)
		# if BOOKDATEStartWithDay > 0:
		# 	print('TCTRIPS BOOKDATEStartWithDay')
		# 	h['TCTRIPS'].BOOKDATE = pd.to_datetime(h['TCTRIPS'].BOOKDATE, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCTRIPS'].BOOKDATE = h['TCTRIPS'].BOOKDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	print(h['TCTRIPS'].BOOKDATE.to_list())
		# 	try:
		# 		h['TCTRIPS'].BOOKDATE = pd.to_datetime(h['TCTRIPS'].BOOKDATE, dayfirst=False, format='%m/%d/%Y')
		# 		h['TCTRIPS'].BOOKDATE = h['TCTRIPS'].BOOKDATE.dt.strftime('%m/%d/%Y')
		# 	except:
		# 		h['TCTRIPS'].BOOKDATE = pd.to_datetime(h['TCTRIPS'].BOOKDATE, dayfirst=False, format='%m/%d/%Y')
		# 		h['TCTRIPS'].BOOKDATE = h['TCTRIPS'].BOOKDATE.dt.strftime('%m/%d/%Y')
       

		# DEPDATEStartWithDay = sum(h['TCTRIPS'].DEPDATE.apply(lambda x: StartWithDay(x)))

		# if DEPDATEStartWithDay > 0:
		# 	print('TCTRIPS DEPDATEStartWithDay')
		# 	h['TCTRIPS'].DEPDATE = pd.to_datetime(h['TCTRIPS'].DEPDATE, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCTRIPS'].DEPDATE = h['TCTRIPS'].DEPDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	print(h['TCTRIPS'].DEPDATE.to_list())
		# 	h['TCTRIPS'].DEPDATE = h['TCTRIPS'].DEPDATE.replace(r'^\s*$', np.nan, regex=True)
		# 	h['TCTRIPS'].DEPDATE = h['TCTRIPS'].DEPDATE.replace(r'00/00/0000', np.nan)
		# 	h['TCTRIPS'].DEPDATE = pd.to_datetime(h['TCTRIPS'].DEPDATE, dayfirst=False, format='%m/%d/%Y')
		# 	h['TCTRIPS'].DEPDATE = h['TCTRIPS'].DEPDATE.dt.strftime('%m/%d/%Y')


		# ARRDATEStartWithDay = sum(h['TCTRIPS'].ARRDATE.apply(lambda x: StartWithDay(x)))

		# if DEPDATEStartWithDay > 0:
		# 	print('TCTRIPS ARRDATEStartWithDay')
		# 	h['TCTRIPS'].ARRDATE = pd.to_datetime(h['TCTRIPS'].ARRDATE, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCTRIPS'].ARRDATE = h['TCTRIPS'].ARRDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	print(h['TCTRIPS'].ARRDATE.to_list())
		# 	h['TCTRIPS'].ARRDATE = h['TCTRIPS'].ARRDATE.replace(r'^\s*$', np.nan, regex=True)
		# 	h['TCTRIPS'].ARRDATE = h['TCTRIPS'].ARRDATE.replace(r'00/00/0000', np.nan)
		# 	h['TCTRIPS'].ARRDATE = pd.to_datetime(h['TCTRIPS'].ARRDATE, dayfirst=False, format='%m/%d/%Y')
		# 	h['TCTRIPS'].ARRDATE = h['TCTRIPS'].ARRDATE.dt.strftime('%m/%d/%Y')


		# fix date formate in TCCARS RENTDATE DATEBACK
		# DATEBACKStartWithDay = sum(h['TCCARS'].DATEBACK.apply(lambda x: StartWithDay(x)))
		# if DATEBACKStartWithDay > 0:
		# 	print('TCCARS DATEBACKStartWithDay')
		# 	h['TCCARS'].DATEBACK = pd.to_datetime(h['TCCARS'].DATEBACK, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCCARS'].DATEBACK = h['TCCARS'].DATEBACK.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	h['TCCARS'].DATEBACK = pd.to_datetime(h['TCCARS'].DATEBACK, dayfirst=False, format='%m/%d/%Y')
		# 	h['TCCARS'].DATEBACK = h['TCCARS'].DATEBACK.dt.strftime('%m/%d/%Y')

		# RENTDATEStartWithDay = sum(h['TCCARS'].RENTDATE.apply(lambda x: StartWithDay(x)))
		# if RENTDATEStartWithDay > 0:
		# 	print('TCCARS RENTDATEartWithDay')
		# 	h['TCCARS'].RENTDATE = pd.to_datetime(h['TCCARS'].RENTDATE, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCCARS'].RENTDATE = h['TCCARS'].RENTDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	h['TCCARS'].RENTDATE = pd.to_datetime(h['TCCARS'].RENTDATE, dayfirst=False, format='%m/%d/%Y')
		# 	h['TCCARS'].RENTDATE = h['TCCARS'].RENTDATE.dt.strftime('%m/%d/%Y')

		# fix date formate in TCHOTEL DATEIN DATEOUT
		# DATEINStartWithDay = sum(h['TCHOTEL'].DATEIN.apply(lambda x: StartWithDay(x)))
		
		# if DATEINStartWithDay > 0:
		# 	print('TCHOTEL DATEBACKStartWithDay')
		# 	h['TCHOTEL'].DATEIN = pd.to_datetime(h['TCHOTEL'].DATEIN, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCHOTEL'].DATEIN = h['TCHOTEL'].DATEIN.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	h['TCHOTEL'].DATEIN = pd.to_datetime(h['TCHOTEL'].DATEIN, dayfirst=False, format='%m/%d/%Y')
		# 	h['TCHOTEL'].DATEIN = h['TCHOTEL'].DATEIN.dt.strftime('%m/%d/%Y')


		# DATEOUTStartWithDay = sum(h['TCHOTEL'].DATEOUT.apply(lambda x: StartWithDay(x)))
		
		# if DATEOUTStartWithDay > 0:
		# 	print('TCHOTEL RENTDATEartWithDay')
		# 	h['TCHOTEL'].DATEOUT = pd.to_datetime(h['TCHOTEL'].DATEOUT, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCHOTEL'].DATEOUT = h['TCHOTEL'].DATEOUT.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	h['TCHOTEL'].DATEOUT = pd.to_datetime(h['TCHOTEL'].DATEOUT, dayfirst=False, format='%m/%d/%Y')
		# 	h['TCHOTEL'].DATEOUT = h['TCHOTEL'].DATEOUT.dt.strftime('%m/%d/%Y')
    
		# fix date formate in TCLEGS RDEPDATE RARRDATE
		# RDEPDATEStartWithDay = sum(h['TCLEGS'].RDEPDATE.apply(lambda x: StartWithDay(x)))

		# if RDEPDATEStartWithDay > 0:
		# 	print('TCLEGS DATEBACKStartWithDay')
		# 	h['TCLEGS'].RDEPDATE = pd.to_datetime(h['TCLEGS'].RDEPDATE, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCLEGS'].RDEPDATE = h['TCLEGS'].RDEPDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	h['TCLEGS'].RDEPDATE = h['TCLEGS'].RDEPDATE.replace(r'^\s*$', np.nan, regex=True)
		# 	h['TCLEGS'].RDEPDATE = h['TCLEGS'].RDEPDATE.replace(r'00/00/0000', np.nan)
		# 	h['TCLEGS'].RDEPDATE = h['TCLEGS'].RDEPDATE.replace(r'//', np.nan)
		# 	h['TCLEGS'].RDEPDATE = pd.to_datetime(h['TCLEGS'].RDEPDATE, dayfirst=False, format='%m/%d/%Y')
		# 	h['TCLEGS'].RDEPDATE = h['TCLEGS'].RDEPDATE.dt.strftime('%m/%d/%Y')


		# RARRDATEStartWithDay = sum(h['TCLEGS'].RARRDATE.apply(lambda x: StartWithDay(x)))

		# if RARRDATEStartWithDay > 0:
		# 	print('TCLEGS RENTDATEartWithDay')
		# 	h['TCLEGS'].RARRDATE = pd.to_datetime(h['TCLEGS'].RARRDATE, dayfirst=True, format='%d/%m/%Y')
		# 	h['TCLEGS'].RARRDATE = h['TCLEGS'].RARRDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	h['TCLEGS'].RARRDATE = h['TCLEGS'].RARRDATE.replace(r'^\s*$', np.nan, regex=True)
		# 	h['TCLEGS'].RARRDATE = h['TCLEGS'].RARRDATE.replace(r'00/00/0000', np.nan)
		# 	h['TCLEGS'].RARRDATE = pd.to_datetime(h['TCLEGS'].RARRDATE, dayfirst=False, format='%m/%d/%Y',errors = 'coerce')
		# 	h['TCLEGS'].RARRDATE = h['TCLEGS'].RARRDATE.dt.strftime('%m/%d/%Y')



		# Fix service

		try:
			print("-----in try block-----")
			TRANDATEStartWithDay = sum(h['TCSERVICES'].TRANDATE.apply(lambda x: StartWithDay(x)))

			if TRANDATEStartWithDay > 0:
				print('TCSERVICES RENTDATEartWithDay')
				h['TCSERVICES'].TRANDATE = pd.to_datetime(h['TCSERVICES'].TRANDATE, dayfirst=True, format='%d/%m/%Y')
				h['TCSERVICES'].TRANDATE = h['TCSERVICES'].TRANDATE.dt.strftime('%m/%d/%Y')
				StartWithDayCount += 1
			# Start with month
			else:
				h['TCSERVICES'].TRANDATE = pd.to_datetime(h['TCSERVICES'].TRANDATE, dayfirst=False, format='%m/%d/%Y')
				h['TCSERVICES'].TRANDATE = h['TCSERVICES'].TRANDATE.dt.strftime('%m/%d/%Y')
		except Exception as e:
			print(h.keys(),"-------in except block----")
			try:
				TRANDATEStartWithDay = sum(h['TCSVCFEES'].TRANDATE.apply(lambda x: StartWithDay(x)))
				if TRANDATEStartWithDay > 0:
					print('RENTDATEartWithDay')
					h['TCSVCFEES'].TRANDATE = pd.to_datetime(h['TCSVCFEES'].TRANDATE, dayfirst=True, format='%d/%m/%Y')
					h['TCSVCFEES'].TRANDATE = h['TCSVCFEES'].TRANDATE.dt.strftime('%m/%d/%Y')
					StartWithDayCount += 1
				# Start with month
				else:
					h['TCSVCFEES'].TRANDATE = pd.to_datetime(h['TCSVCFEES'].TRANDATE, dayfirst=False, format='%m/%d/%Y')
					h['TCSVCFEES'].TRANDATE = h['TCSVCFEES'].TRANDATE.dt.strftime('%m/%d/%Y')
			except:

				TRANDATEStartWithDay = sum(h['TCSERVICES'].TRANDATE.apply(lambda x: StartWithDay(x)))
				if TRANDATEStartWithDay > 0:
					print('RENTDATEartWithDay')
					h['TCSERVICES'].TRANDATE = pd.to_datetime(h['TCSERVICES'].TRANDATE,  format='%Y-%m-%d')
					h['TCSERVICES'].TRANDATE = h['TCSERVICES'].TRANDATE.dt.strftime('%m/%d/%Y')
					StartWithDayCount += 1
				# Start with month
				else:
					h['TCSERVICES'].TRANDATE = pd.to_datetime(h['TCSERVICES'].TRANDATE)
					h['TCSERVICES'].TRANDATE = h['TCSERVICES'].TRANDATE.dt.strftime('%m/%d/%Y')

		print(h.keys())
		print('TCLEGS StartWithDayCount', StartWithDayCount)
		# exit()
		# check if start with date but didnt convert them all to 'd/m/y'
		if StartWithDayCount > 0 and StartWithDayCount != 11:
			# print "h['TCTRIPS'].INVDATE ",h['TCTRIPS'].INVDATE.to_list()
			# print "h['TCTRIPS'].BOOKDATE ",h['TCTRIPS'].BOOKDATE.to_list()
			# print "h['TCTRIPS'].DEPDATE ",h['TCTRIPS'].DEPDATE.to_list()
			# print "h['TCTRIPS'].ARRDATE ",h['TCTRIPS'].ARRDATE.to_list()
			# print "h['TCCARS'].DATEBACK  ",h['TCCARS'].DATEBACK.to_list()
			# print "h['TCCARS'].RENTDATE ",h['TCCARS'].RENTDATE.to_list()
			# print "h['TCHOTEL'].DATEIN  ",h['TCHOTEL'].DATEIN.to_list()
			# print "h['TCHOTEL'].DATEOUT  ",h['TCHOTEL'].DATEOUT.to_list()
			# print "h['TCLEGS'].RDEPDATE  ",h['TCLEGS'].RDEPDATE.to_list()
			# print "h['TCLEGS'].RARRDATE  ",h['TCLEGS'].RARRDATE.to_list()
			input('please Check your dates press any key to continue')

		print(d.keys())
		print(d['TCTRIPS'].head())
		
		# StartWithDayCount = 0
		# INVDATEtartWithDay = sum(d['TCTRIPS'].INVDATE.apply(lambda x: StartWithDay(x)))
		# print(INVDATEtartWithDay)
		# print(d['TCTRIPS'].INVDATE)
		# if INVDATEtartWithDay > 0:
		# 	print('TCTRIPS INVDATEtartWithDay')
		# 	print(d['TCTRIPS'].INVDATE.tolist())
		# 	d['TCTRIPS'].INVDATE = pd.to_datetime(d['TCTRIPS'].INVDATE, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCTRIPS'].INVDATE = d['TCTRIPS'].INVDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	d['TCTRIPS'].INVDATE = pd.to_datetime(d['TCTRIPS'].INVDATE, dayfirst=False, format='%m/%d/%Y',
		# 										  errors='coerce')
		# 	d['TCTRIPS'].INVDATE = d['TCTRIPS'].INVDATE.dt.strftime('%m/%d/%Y')

		# # print(d['TCTRIPS'].INVDATE)
		# # exit()
		# BOOKDATEStartWithDay = sum(d['TCTRIPS'].BOOKDATE.apply(lambda x: StartWithDay(x)))
		# if BOOKDATEStartWithDay > 0:
		# 	print('TCTRIPS BOOKDATEStartWithDay')
		# 	d['TCTRIPS'].BOOKDATE = pd.to_datetime(d['TCTRIPS'].BOOKDATE, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCTRIPS'].BOOKDATE = d['TCTRIPS'].BOOKDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	d['TCTRIPS'].BOOKDATE = pd.to_datetime(d['TCTRIPS'].BOOKDATE, dayfirst=False, format='%m/%d/%Y')
		# 	d['TCTRIPS'].BOOKDATE = d['TCTRIPS'].BOOKDATE.dt.strftime('%m/%d/%Y')

		# DEPDATEStartWithDay = sum(d['TCTRIPS'].DEPDATE.apply(lambda x: StartWithDay(x)))
		

		# if DEPDATEStartWithDay > 0:
		# 	print('TCTRIPS DEPDATEStartWithDay')
		# 	d['TCTRIPS'].DEPDATE = pd.to_datetime(d['TCTRIPS'].DEPDATE, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCTRIPS'].DEPDATE = d['TCTRIPS'].DEPDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	print(d['TCTRIPS'].DEPDATE.to_list())
		# 	d['TCTRIPS'].DEPDATE = d['TCTRIPS'].DEPDATE.replace(r'^\s*$', np.nan, regex=True)
		# 	d['TCTRIPS'].DEPDATE = d['TCTRIPS'].DEPDATE.replace(r'00/00/0000', np.nan)
		# 	d['TCTRIPS'].DEPDATE = pd.to_datetime(d['TCTRIPS'].DEPDATE, dayfirst=False, format='%m/%d/%Y')
		# 	d['TCTRIPS'].DEPDATE = d['TCTRIPS'].DEPDATE.dt.strftime('%m/%d/%Y')


		# ARRDATEStartWithDay = sum(d['TCTRIPS'].ARRDATE.apply(lambda x: StartWithDay(x)))

		# if DEPDATEStartWithDay > 0:
		# 	print('TCTRIPS ARRDATEStartWithDay')
		# 	d['TCTRIPS'].ARRDATE = pd.to_datetime(d['TCTRIPS'].ARRDATE, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCTRIPS'].ARRDATE = d['TCTRIPS'].ARRDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	print(d['TCTRIPS'].ARRDATE.to_list())
		# 	d['TCTRIPS'].ARRDATE = d['TCTRIPS'].ARRDATE.replace(r'^\s*$', np.nan, regex=True)
		# 	d['TCTRIPS'].ARRDATE = d['TCTRIPS'].ARRDATE.replace(r'00/00/0000', np.nan)
		# 	d['TCTRIPS'].ARRDATE = pd.to_datetime(d['TCTRIPS'].ARRDATE, dayfirst=False, format='%m/%d/%Y')
		# 	d['TCTRIPS'].ARRDATE = d['TCTRIPS'].ARRDATE.dt.strftime('%m/%d/%Y')


		# # fix date formate in TCCARS RENTDATE DATEBACK
		# DATEBACKStartWithDay = sum(d['TCCARS'].DATEBACK.apply(lambda x: StartWithDay(x)))
		# if DATEBACKStartWithDay > 0:
		# 	print('TCCARS DATEBACKStartWithDay')
		# 	d['TCCARS'].DATEBACK = pd.to_datetime(d['TCCARS'].DATEBACK, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCCARS'].DATEBACK = d['TCCARS'].DATEBACK.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	d['TCCARS'].DATEBACK = pd.to_datetime(d['TCCARS'].DATEBACK, dayfirst=False, format='%m/%d/%Y')
		# 	d['TCCARS'].DATEBACK = d['TCCARS'].DATEBACK.dt.strftime('%m/%d/%Y')

		# RENTDATEStartWithDay = sum(d['TCCARS'].RENTDATE.apply(lambda x: StartWithDay(x)))
		# if RENTDATEStartWithDay > 0:
		# 	print('TCCARS RENTDATEartWithDay')
		# 	d['TCCARS'].RENTDATE = pd.to_datetime(d['TCCARS'].RENTDATE, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCCARS'].RENTDATE = d['TCCARS'].RENTDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	d['TCCARS'].RENTDATE = pd.to_datetime(d['TCCARS'].RENTDATE, dayfirst=False, format='%m/%d/%Y')
		# 	d['TCCARS'].RENTDATE = d['TCCARS'].RENTDATE.dt.strftime('%m/%d/%Y')

		# # fix date formate in TCHOTEL DATEIN DATEOUT
		# DATEINStartWithDay = sum(d['TCHOTEL'].DATEIN.apply(lambda x: StartWithDay(x)))
		# if DATEINStartWithDay > 0:
		# 	print('TCHOTEL DATEBACKStartWithDay')
		# 	d['TCHOTEL'].DATEIN = pd.to_datetime(d['TCHOTEL'].DATEIN, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCHOTEL'].DATEIN = d['TCHOTEL'].DATEIN.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	d['TCHOTEL'].DATEIN = pd.to_datetime(d['TCHOTEL'].DATEIN, dayfirst=False, format='%m/%d/%Y')
		# 	d['TCHOTEL'].DATEIN = d['TCHOTEL'].DATEIN.dt.strftime('%m/%d/%Y')


		# DATEOUTStartWithDay = sum(d['TCHOTEL'].DATEOUT.apply(lambda x: StartWithDay(x)))
		# if DATEOUTStartWithDay > 0:
		# 	print('TCHOTEL RENTDATEartWithDay')
		# 	d['TCHOTEL'].DATEOUT = pd.to_datetime(d['TCHOTEL'].DATEOUT, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCHOTEL'].DATEOUT = d['TCHOTEL'].DATEOUT.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	d['TCHOTEL'].DATEOUT = pd.to_datetime(d['TCHOTEL'].DATEOUT, dayfirst=False, format='%m/%d/%Y')
		# 	d['TCHOTEL'].DATEOUT = d['TCHOTEL'].DATEOUT.dt.strftime('%m/%d/%Y')

		# # fix date formate in TCLEGS RDEPDATE RARRDATE
		# RDEPDATEStartWithDay = sum(d['TCLEGS'].RDEPDATE.apply(lambda x: StartWithDay(x)))

		# if RDEPDATEStartWithDay > 0:
		# 	print('TCLEGS DATEBACKStartWithDay')
		# 	d['TCLEGS'].RDEPDATE = pd.to_datetime(d['TCLEGS'].RDEPDATE, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCLEGS'].RDEPDATE = d['TCLEGS'].RDEPDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	d['TCLEGS'].RDEPDATE = d['TCLEGS'].RDEPDATE.replace(r'^\s*$', np.nan, regex=True)
		# 	d['TCLEGS'].RDEPDATE = d['TCLEGS'].RDEPDATE.replace(r'00/00/0000', np.nan)
		# 	d['TCLEGS'].RDEPDATE = d['TCLEGS'].RDEPDATE.replace(r'//', np.nan)
		# 	d['TCLEGS'].RDEPDATE = pd.to_datetime(d['TCLEGS'].RDEPDATE, dayfirst=False, format='%m/%d/%Y')
		# 	d['TCLEGS'].RDEPDATE = d['TCLEGS'].RDEPDATE.dt.strftime('%m/%d/%Y')


		# RARRDATEStartWithDay = sum(d['TCLEGS'].RARRDATE.apply(lambda x: StartWithDay(x)))

		# if RARRDATEStartWithDay > 0:
		# 	print('TCLEGS RENTDATEartWithDay')
		# 	d['TCLEGS'].RARRDATE = pd.to_datetime(d['TCLEGS'].RARRDATE, dayfirst=True, format='%d/%m/%Y')
		# 	d['TCLEGS'].RARRDATE = d['TCLEGS'].RARRDATE.dt.strftime('%m/%d/%Y')
		# 	StartWithDayCount += 1
		# # Start with month
		# else:
		# 	d['TCLEGS'].RARRDATE = d['TCLEGS'].RARRDATE.replace(r'^\s*$', np.nan, regex=True)
		# 	d['TCLEGS'].RARRDATE = d['TCLEGS'].RARRDATE.replace(r'00/00/0000', np.nan)
		# 	d['TCLEGS'].RARRDATE = pd.to_datetime(d['TCLEGS'].RARRDATE, dayfirst=False, format='%m/%d/%Y',errors = 'coerce')
		# 	d['TCLEGS'].RARRDATE = d['TCLEGS'].RARRDATE.dt.strftime('%m/%d/%Y')



		# Fix service

		try:
			TRANDATEStartWithDay = sum(d['TCSERVICES'].TRANDATE.apply(lambda x: StartWithDay(x)))

			if TRANDATEStartWithDay > 0:
				print('TCSERVICES RENTDATEartWithDay')
				d['TCSERVICES'].TRANDATE = pd.to_datetime(d['TCSERVICES'].TRANDATE, dayfirst=True, format='%d/%m/%Y')
				d['TCSERVICES'].TRANDATE = d['TCSERVICES'].TRANDATE.dt.strftime('%m/%d/%Y')
				StartWithDayCount += 1
			# Start with month
			else:
				d['TCSERVICES'].TRANDATE = pd.to_datetime(d['TCSERVICES'].TRANDATE, dayfirst=False, format='%m/%d/%Y')
				d['TCSERVICES'].TRANDATE = d['TCSERVICES'].TRANDATE.dt.strftime('%m/%d/%Y')
		except Exception as e:
			try:
				TRANDATEStartWithDay = sum(d['TCSVCFEES'].TRANDATE.apply(lambda x: StartWithDay(x)))
				if TRANDATEStartWithDay > 0:
					print('RENTDATEartWithDay')
					d['TCSVCFEES'].TRANDATE = pd.to_datetime(d['TCSVCFEES'].TRANDATE, dayfirst=True, format='%d/%m/%Y')
					d['TCSVCFEES'].TRANDATE = d['TCSVCFEES'].TRANDATE.dt.strftime('%m/%d/%Y')
					StartWithDayCount += 1
				# Start with month
				else:
					d['TCSVCFEES'].TRANDATE = pd.to_datetime(d['TCSVCFEES'].TRANDATE, dayfirst=False, format='%m/%d/%Y')
					d['TCSVCFEES'].TRANDATE = d['TCSVCFEES'].TRANDATE.dt.strftime('%m/%d/%Y')
			except:
				TRANDATEStartWithDay = sum(d['TCSERVICES'].TRANDATE.apply(lambda x: StartWithDay(x)))
				if TRANDATEStartWithDay > 0:
					print('RENTDATEartWithDay')
					d['TCSERVICES'].TRANDATE = pd.to_datetime(d['TCSERVICES'].TRANDATE, dayfirst=True, format='%Y-%m-%d')
					d['TCSERVICES'].TRANDATE = d['TCSERVICES'].TRANDATE.dt.strftime('%m/%d/%Y')
					StartWithDayCount += 1
				# Start with month
				else:
					print(d['TCSERVICES'].TRANDATE)
					try:
						d['TCSERVICES'].TRANDATE = pd.to_datetime(d['TCSERVICES'].TRANDATE, dayfirst=False, format='%Y-%m-%d')
						d['TCSERVICES'].TRANDATE = d['TCSERVICES'].TRANDATE.dt.strftime('%m/%d/%Y')
					except:
						pass
		print(d.keys())
		print('TCLEGS StartWithDayCount', StartWithDayCount)
		# check if start with date but didnt convert them all to 'd/m/y'
		if StartWithDayCount > 0 and StartWithDayCount != 11:
			# print "d['TCTRIPS'].INVDATE ",d['TCTRIPS'].INVDATE.to_list()
			# print "d['TCTRIPS'].BOOKDATE ",d['TCTRIPS'].BOOKDATE.to_list()
			# print "d['TCTRIPS'].DEPDATE ",d['TCTRIPS'].DEPDATE.to_list()
			# print "d['TCTRIPS'].ARRDATE ",d['TCTRIPS'].ARRDATE.to_list()
			# print "d['TCCARS'].DATEBACK  ",d['TCCARS'].DATEBACK.to_list()
			# print "d['TCCARS'].RENTDATE ",d['TCCARS'].RENTDATE.to_list()
			# print "d['TCHOTEL'].DATEIN  ",d['TCHOTEL'].DATEIN.to_list()
			# print "d['TCHOTEL'].DATEOUT  ",d['TCHOTEL'].DATEOUT.to_list()
			# print "d['TCLEGS'].RDEPDATE  ",d['TCLEGS'].RDEPDATE.to_list()
			# print "d['TCLEGS'].RARRDATE  ",d['TCLEGS'].RARRDATE.to_list()
			input('please Check your dates press any key to continue')





		# [FS] end correct the date format

		# This line deletes the folder.  It remains within the outer for loop (for folder in folders...)

		rmtree(folder)

	# This line creates a new folder in the directory with today's date as the name.  This is the folder that the clean files will be written to.
	# It is important that this line be after and outside the 'for folder in folders...' loop.  You don't want the file read and deleted,
	# as it would be if this line came before, nor do you want the file to be made and overwritten over and over again.
	#print('ok')
	
	os.mkdir('%s' % date)
	
	# print h['TCUDIDS']['UDIDNO']
	# exit()
	# print(h['TCUDIDS']['UDIDNO'].unique(),'444444444444444444444444444444444444444444')
	# print('---------------------------------------------------------------------------------------')


	deleted_records = []

	bo = []
	eqx = []
	print("yaya yoyo=", d)
	# exit()
	trips = d['TCTRIPS'].fillna('')
	print("HEHEHhehe =", trips)
	# exit()
	# fix ATPI FRANCE Accounts inside trips
	acct_to_fix = {"180882":"882","180883":"883","180884":"884","180885":"885","182458":"2458"}

	trips['ACCT'] = trips['ACCT'].apply(lambda x: acct_to_fix[str(x)] if str(x) in acct_to_fix else x)
	
	# this dictionary will be used to fetch 'ACCT' from trips by giving reckey
	trips_acct = pd.Series(trips['ACCT'].values,index=trips.RECKEY).to_dict()
	
	# List of countries whom date will be fixed using the country code provided in list
	fixCountriesDateList = ['110','115','158','344','273','261','300','320','279']
	# fixCountriesDateList = ['110','115','158','344','273']
	#print ("Total TRIPS = %d" % len(trips))

	# Following loop filter the BO records and EQX records from trips data.
	# if len(trips) > 0:
	# 	trips['BOOKINGNBR'] = trips.apply(lambda x:createBNBR(x['BOOKINGNBR'],x['RECKEY'],x['PLUSMIN']),axis=1)
	trips.loc[ (trips['BOOKINGNBR'].str.len() > 4) & ( ~trips['BOOKINGNBR'].str.startswith('B') ), 'BOOKINGNBR' ] = ''
	for i,row in trips.iterrows():
		# print(row['RECKEY'])
		if len(str(row['BOOKINGNBR'])) != 0:
			if str(row['BOOKINGNBR']) != '' and str(row['BOOKINGNBR'])[0] != 'E' and str(row['BOOKINGNBR'])[0] != 'B':
				CC=_country_number_dict.get(int((row['RECKEY'])[0:3]))
				lastChar = 'R' if str(row['PLUSMIN']) == '-1' else 'S'
				trips.loc[i,'BOOKINGNBR']= ''.join(['B{}'.format(CC),lastChar])
			elif str(row['BOOKINGNBR'])[0] == 'E' or (str(row['BOOKINGNBR'])[0] == 'B' and len(str(row['BOOKINGNBR'])[0]) == 4):
				trips.loc[i,'BOOKINGNBR'] = row['BOOKINGNBR']
			elif len(str(row['BOOKINGNBR'])) > 4 and str(row['BOOKINGNBR'])[4] == '-':
				if str(row['BOOKINGNBR'])[0] == 'B' and str(row['BOOKINGNBR'])[4] == '-':
					trips.loc[i,'BOOKINGNBR'] = row['BOOKINGNBR'].split('-',1)[0]
			else:
				CC=_country_number_dict.get(int((row['RECKEY'])[0:3]))
				lastChar = 'R' if str(row['PLUSMIN']) == '-1' else 'S'
				trips.loc[i,'BOOKINGNBR']= ''.join(['B{}'.format(CC),lastChar])
		else:
			CC=_country_number_dict.get(int((row['RECKEY'])[0:3]))
			lastChar = 'R' if str(row['PLUSMIN']) == '-1' else 'S'
			trips.loc[i,'BOOKINGNBR']= ''.join(['B{}'.format(CC),lastChar])

	d['TCTRIPS'] = trips

	# The following segments deal with cleaning and processing the consolidated data after it has been read.
	# This next segment processes the TCCARS data.
	keywordNotFound = []
	cars = d['TCCARS']
	# print "Hello you are here"
	# print "Car File length", len(cars)
	# exit()
	# exit()		
	# These pieces of code (if len(table) != 0) are important for avoiding errors when you are running the batchscript on one file
	# and one of the files is empty. "Table.applymap" and "for i, row in tables.iterrows()" will return errors when run one empty DataFrames.
	
	if len(cars) != 0:
		# The next two lines do basic cleaning, capitalizing all letters, removing excess spaces and commas, and replacing null values.
		print("Welcom to Cars Section")
		# exit()		
		cars = cars.applymap(lambda x: str(x).upper().strip().replace(',','').replace('NAN',''))
		# cars = cars.applymap(lambda x:str(x).upper().strip().replace(',','').replace('NAN','') if x == "" or x=="nan" else str(x).upper().strip().replace(',',''))
		cars = cars.fillna('')

		# This array stores the reckeys of the records that will need to be removed.

		dropped_cars = []

		cars['RENTDATE'] = cars['RENTDATE'].apply(lambda x: remInvalidDates(str(x)))
		cars['DATEBACK'] = cars['DATEBACK'].apply(lambda x: remInvalidDates(str(x)))

		cars['RENTDATE'] = cars['RENTDATE'].apply(lambda x: convert_date(x) if '-' in str(x) else x)
		cars['DATEBACK'] = cars['DATEBACK'].apply(lambda x: convert_date(x) if '-' in str(x) else x)
		
		cars['RENTDATE'] = cars['RENTDATE'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)
		cars['DATEBACK'] = cars['DATEBACK'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)
		cars['RENTDATE'] = pd.to_datetime(cars['RENTDATE'])
		cars['RENTDATE'] = cars['RENTDATE'].dt.strftime('%m/%d/%Y')
		cars['DATEBACK'] = pd.to_datetime(cars['DATEBACK'])
		cars['DATEBACK'] = cars['DATEBACK'].dt.strftime('%m/%d/%Y')
		# print cars['DATEBACK']
		# exit()
		
		cars['RENTDATE'] = cars['RENTDATE'].apply(lambda x: x if str(x) != 'NaT' else '')
		cars['DATEBACK'] = cars['DATEBACK'].apply(lambda x: x if str(x) != 'NaT' else '')
		# The iterrows function loops through each row in the DataFrame, with changes you made being run at the end in the order they are listed.
		# print cars['DAYS']
		# exit("Hello Cars")
		
		cars['ABOOKRAT']  = cars['ABOOKRAT'].astype(float)
		cars['DAYS']  = cars['DAYS'].astype(float)
		cars.loc[ (cars['RECKEY'].str.startswith('217')) | (cars['RECKEY'].str.startswith('225')) , 'ABOOKRAT'] = round(cars['ABOOKRAT'] / cars['DAYS'], 2)
		""" if str(row['RECKEY'])[0:3] == '217' or str(row['RECKEY'])[0:3] == '225':
				cars.loc[i,'ABOOKRAT'] = float(row['ABOOKRAT']) / float(row['DAYS']) """
		# update RATETYPE TO 'DY' WHERE RATETYPE IS 'DAY'
		cars.loc[cars['RATETYPE'] == 'DAY', 'RATETYPE'] = 'DY'
		""" if row['RATETYPE'] == 'DAY':
				cars.loc[i,'RATETYPE'] = 'DY' """
		# update ABOOKRAT TO float(row['ABOOKRAT']) / 7.0 WHERE ABOOKRAT is greater than 180

		cars.loc[ (cars['RATETYPE'] == 'DY') & (cars['ABOOKRAT'] > 180.0) , 'ABOOKRAT'] = cars['ABOOKRAT'] / 7.0
		""" if row['RATETYPE'] == 'DY':
				if float(row['ABOOKRAT']) > 180:
					cars.loc[i,'ABOOKRAT'] = float(row['ABOOKRAT']) / 7.0 """

		cars.loc[ (cars['RATETYPE'] == 'WK') & (cars['ABOOKRAT'] > 0) , 'RATETYPE'] = 'DY'
		cars.loc[ (cars['RATETYPE'] == 'WK') & (cars['ABOOKRAT'] > 0) , 'ABOOKRAT'] = cars['ABOOKRAT'] / 7.0
		""" if row['RATETYPE'] == 'WK' and float(row['ABOOKRAT']) > 0:
				cars.loc[i,'ABOOKRAT'] = float(row['ABOOKRAT']) / 7.0 
				cars.loc[i,'RATETYPE'] = 'DY'
		"""
		cars.loc[ cars['RATETYPE'] == '' , 'RATETYPE'] = 'DY'
		""" if row['RATETYPE'] == '':
				cars.loc[i,'RATETYPE'] = 'DY' """
		# update RATETYPE TO 'DY' WHERE RATETYPE = 'WK' and ABOOKRAT = 0
		cars.loc[ (cars['RATETYPE'] == 'WK') & (cars['ABOOKRAT'] == 0) , 'RATETYPE'] = 'DY'
		""" 
		if row['RATETYPE'] == 'WK' and float(row['ABOOKRAT']) == 0:
				cars.loc[i,'RATETYPE'] = 'DY'
		"""
		# update RATETYPE TO 'DY' WHERE RATETYPE is not 'WK' and rate type is not 'DY' AND MONEYTYPE IN ['AUD', 'CAD', 'USD', 'EUR'] and ABOOKRAT < 180
		cars.loc[ (cars['RATETYPE'] != 'WK') & (cars['RATETYPE'] != 'DY') & (cars['MONEYTYPE'].isin(['AUD', 'CAD', 'USD', 'EUR'])) & (cars['ABOOKRAT'] < 180.0) , 'RATETYPE'] = 'DY'
		cars.loc[ (cars['RATETYPE'] != 'WK') & (cars['RATETYPE'] != 'DY') & (cars['MONEYTYPE'].isin(['AUD', 'CAD', 'USD', 'EUR'])) & (cars['ABOOKRAT'] >= 180.0) , 'RATETYPE'] = 'DY'
		cars.loc[ (cars['RATETYPE'] != 'WK') & (cars['RATETYPE'] != 'DY') & (cars['MONEYTYPE'].isin(['AUD', 'CAD', 'USD', 'EUR'])) & (cars['ABOOKRAT'] >= 180.0) , 'ABOOKRAT'] = cars['ABOOKRAT'] / 7.0

		""" if row['RATETYPE'] != 'DY' and row['RATETYPE'] != 'WK':
				if row['MONEYTYPE'] == 'AUD' or row['MONEYTYPE'] == 'CAD' or row['MONEYTYPE'] == 'USD' or row['MONEYTYPE'] == 'EUR':
					if float(row['ABOOKRAT']) < 180:
						cars.loc[i,'RATETYPE'] = 'DY'
					else:
						cars.loc[i,'RATETYPE'] = 'DY'
						cars.loc[i,'ABOOKRAT'] = float(row['ABOOKRAT']) / 7.0 """
		# update CPLUSMIN to -1 where ABOOKRAT < 0
		cars.loc[ cars['ABOOKRAT'] < 0 , 'CPLUSMIN'] = '-1'
		# update CPLUSMIN to C where ABOOKRAT < 0
		cars.loc[ cars['ABOOKRAT'] < 0 , 'TRANTYPE'] = 'C'
		# update CPLUSMIN to 1 where ABOOKRAT > 0
		cars.loc[ cars['ABOOKRAT'] > 0 , 'CPLUSMIN'] = '1'
		""" These lines fix the plusmin issue, checking if the value is negative or not, and setting plusmin accordingly.
			if '-' in str(row['ABOOKRAT']):
				cars.loc[i,'CPLUSMIN'] = '-1'
				cars.loc[i,'TRANTYPE'] = 'C'
			else:
				cars.loc[i,'CPLUSMIN'] = '1' """
		
		
		# for i, row in cars.iterrows():
			# print(row)
			# if row['ABOOKRAT'] =='':
			# 	row['ABOOKRAT'] = np.nan
			# if str(row['RECKEY'])[0:3] == '217' or str(row['RECKEY'])[0:3] == '225':
			# 	cars.loc[i,'ABOOKRAT'] = float(row['ABOOKRAT']) / float(row['DAYS'])
			# if row['RATETYPE'] == 'DAY':
			# 	cars.loc[i,'RATETYPE'] = 'DY'
			# if row['RATETYPE'] == 'DY':
			# 	print(row['ABOOKRAT'])
			# 	if float(row['ABOOKRAT']) > 180:
			# 		cars.loc[i,'ABOOKRAT'] = float(row['ABOOKRAT']) / 7.0
			# if row['RATETYPE'] == 'WK' and float(row['ABOOKRAT']) > 0:
			# 	cars.loc[i,'ABOOKRAT'] = float(row['ABOOKRAT']) / 7.0
			# 	cars.loc[i,'RATETYPE'] = 'DY'
			# if row['RATETYPE'] == '':
			# 	cars.loc[i,'RATETYPE'] = 'DY'
			# if row['RATETYPE'] == 'WK' and float(row['ABOOKRAT']) == 0:
			# 	cars.loc[i,'RATETYPE'] = 'DY'
			# if row['RATETYPE'] != 'DY' and row['RATETYPE'] != 'WK':
			# 	if row['MONEYTYPE'] == 'AUD' or row['MONEYTYPE'] == 'CAD' or row['MONEYTYPE'] == 'USD' or row['MONEYTYPE'] == 'EUR':
			# 		if float(row['ABOOKRAT']) < 180:
			# 			cars.loc[i,'RATETYPE'] = 'DY'
			# 		else:
			# 			cars.loc[i,'RATETYPE'] = 'DY'
			# 			cars.loc[i,'ABOOKRAT'] = float(row['ABOOKRAT']) / 7.0

			# # These lines fix the plusmin issue, checking if the value is negative or not, and setting plusmin accordingly.
			# if '-' in str(row['ABOOKRAT']):
			# 	cars.loc[i,'CPLUSMIN'] = '-1'
			# 	cars.loc[i,'TRANTYPE'] = 'C'
			# else:
			# 	cars.loc[i,'CPLUSMIN'] = '1'

			# These lines convert the date from d/m/Y to m/d/Y, according to their country code.
			# This has to be hardcoded in, as there is no programmatic way of determining what date format is used in a consistent way.
			# [fs] fix date format
			# if str(row['RECKEY'])[0:3] in fixCountriesDateList:
				
			# 	try:
			# 		cars.loc[i,'RENTDATE'] = datetime.datetime.strptime(row['RENTDATE'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 		cars.loc[i,'DATEBACK'] = datetime.datetime.strptime(row['DATEBACK'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 	except ValueError:
			# 		cars.loc[i,'RENTDATE'] = datetime.datetime.strptime(row['RENTDATE'].replace('.','/'),'%m/%d/%Y').strftime('%m/%d/%Y')
			# 		cars.loc[i,'DATEBACK'] = datetime.datetime.strptime(row['DATEBACK'].replace('.','/'),'%m/%d/%Y').strftime('%m/%d/%Y')

			# This next block fixes the city, state (country), and metro fields, using the airport dictionary.
			# First it checks if the metro code is in the dictionary, and if so populates the other fields accordingly.
			# Next it checks to see if the metro code was placed in the city column by mistake, and if so, fixes the values.
			# Finally, it does a reverse dictionary lookup, to see if it can match the city and state values to the right key.
			# In order to make this last task work Portland US PDX had to be hardcoded as the right values for anything that vaguely matches.
			# print('==========================================')
			# print(row['CITYCODE'],row['AUTOCITY'])
			# print('==========================================')
		
		for i, row in cars.iterrows():
			if row['CITYCODE'] in airport_dict:
				site_info = airport_dict[row['CITYCODE']]
				cars.loc[i, 'AUTOCITY'] = site_info[0]
				cars.loc[i, 'AUTOSTAT'] = site_info[-1]
			elif row['AUTOCITY'] in airport_dict:
				site_info = airport_dict[row['AUTOCITY']]
				cars.loc[i, 'CITYCODE'] = row['AUTOCITY']
				cars.loc[i, 'AUTOCITY'] = site_info[0]
				cars.loc[i, 'AUTOSTAT'] = site_info[-1]
			else:
				for k, v in airport_dict.items():
					if v[0] == row['AUTOCITY'] and v[-1] == row['AUTOSTAT']:
						cars.loc[i, 'CITYCODE'] = k
					elif row['AUTOCITY'] == 'PORTLAND':
						cars.loc[i, 'CITYCODE'] = 'PDX'
						cars.loc[i, 'AUTOSTAT'] = 'US'
					elif v[0] == row['AUTOCITY'] and row['AUTOSTAT'] == '':
						cars.loc[i, 'CITYCODE'] = k
						cars.loc[i, 'AUTOSTAT'] = v[-1]
			
			# Using LUT Car Company Code file: Update Company accoding to carcode and update cartype if carcode is 'LM'
			if row['CARCODE'] in car_code_to_company:
				cars.loc[i,'COMPANY'] = car_code_to_company[row['CARCODE']]
			else:
				for k,v in car_keyword_to_code.items():
					if k in row['COMPANY']:
						# if car_keyword_to_code[k] == 'LM':
						# 	cars.loc[i,'CARTYPE'] = 'LIMO'
						cars.loc[i,'CARCODE'] = car_keyword_to_code[k]
						cars.loc[i,'COMPANY'] = car_keyword_to_company[k]
						if car_keyword_to_cartype[k] == 'LIMO':
							cars.loc[i,'CARTYPE'] = 'LIMO'
					else:
						keywordNotFound.append(row['RECKEY']) 
				
			flag = False
			if str(row['MONEYTYPE']) in currencies:
				flag = True

			if not flag:
				if row['RECKEY'] in trips_acct:
					if str(trips_acct[row['RECKEY']]) in lut_currencies:
						cars.loc[i,'MONEYTYPE'] = lut_currencies[str(trips_acct[row['RECKEY']])]

		# carsCompany = pd.unique(pd.Series(cars['COMPANY'].values).dropna())
		# If none of the above worked, the record key is added the the dropped cars array and then removed from the DataFrame.
		for i,row in cars.iterrows():
			if row['CITYCODE'] not in airport_dict:               
				error_report.append(['TEMP','TCCARS',row['RECKEY'],'%s %s %s not a valid entry' % (row['AUTOCITY'],row['AUTOSTAT'],row['CITYCODE'])])
				dropped_cars.append(row['RECKEY'])
			if row['CARCODE'] not in car_code_to_company and row['RECKEY'] in keywordNotFound:
				if 'LIMO' in row['COMPANY']:
					cars.loc[i,'CARCODE'] = 'LS'
					cars.loc[i,'COMPANY'] = 'LIMO'
				else:
					cars.loc[i,'CARCODE'] = 'CR'
					if row['COMPANY'] == '' or str(row['COMPANY']) == 'nan':
						cars.loc[i,'COMPANY'] = 'MISC CAR'
			
		# for i in dropped_cars:
		# 	deleted_records.append([i,'TCCARS.CITYCODE not available in Airport Master file'])
		# 	cars = cars[cars.RECKEY != i]

		# Records are sorted by RECKEY and DAYS, and duplicate records dropped.
		cars = cars.sort_values(by=['RECKEY','DAYS'])

		cars['D1'] = cars.duplicated()

		cars = cars.drop_duplicates()

		cars['D2'] = cars.duplicated(['RECKEY','COMPANY','AUTOCITY','RENTDATE'], keep='first')

		cars = cars.drop_duplicates(['RECKEY','COMPANY','AUTOCITY','RENTDATE'], keep='last')

		for i,row in cars.iterrows():
			if row['D1'] == True:
				deleted_records.append([row['RECKEY'],'Cars:Complete Record Matched'])
			if row['D2'] == True:
				deleted_records.append([row['RECKEY'],'Cars:RECKEY,COMPANY,AUTOCITY & RENTDATE Matched'])
				
		if 'D1' in cars.columns:
			del cars['D1']
		if 'D2' in cars.columns:
			del cars['D2']
	# The DataFrame is then written to a Tccars csv file in the folder named today's date.
	cars.to_csv('{}\Tccars.csv'.format(date),index=False)

	# The block of code for hotels does the exact same things as was done for cars above, except it adds a bit to strip foreign characters out
	# of field names first.  This was necessary because certain countries, namely in Asia, contain non-Latin characters in some of their fields
	# which are not supported by our software.
	hotels = d['TCHOTEL']
	ud54 = []

	if len(hotels) != 0:
		print("Welcom to Hotels Section")
		hotels = hotels.applymap(lambda x: str(x).upper().strip().replace(',','').replace('NAN',''))
		# hotels = hotels.applymap(lambda x:str(x).upper().strip().replace(',','').replace('NAN','') if x == "" or x=="nan" else str(x).upper().strip().replace(',',''))
		# hotels = hotels.applymap(lambda x: unidecode(unicode(x, 'latin-1')))
		dropped_hotels = []
		new_text = hotels['HOTCOUNTRY']
		new_text = [country_dict[i] if i in country_dict else i for i in new_text]
		hotels['HOTCOUNTRY'] = new_text
		hotels = hotels.fillna('')
		names = hotels['HOTELNAM']
		p = re.compile(r'[^A-Za-z0-9\s\-]')
		new_names = [p.sub('', x) for x in names]
		hotels['HOTELNAM'] = new_names
		cities = hotels['HOTCITY']
		p = re.compile(r'[^A-Za-z\s\-]')
		new_cities = [p.sub('', x) for x in cities]
		hotels['HOTCITY'] = new_cities

		hotels['DATEIN'] = hotels['DATEIN'].apply(lambda x: remInvalidDates(str(x)))
		hotels['DATEOUT'] = hotels['DATEOUT'].apply(lambda x: remInvalidDates(str(x)))

		hotels['DATEIN'] = hotels['DATEIN'].apply(lambda x: convert_date(x) if '-' in str(x) else x)
		hotels['DATEOUT'] = hotels['DATEOUT'].apply(lambda x: convert_date(x) if '-' in str(x) else x)

		hotels['DATEIN'] = hotels['DATEIN'].apply(lambda x: str(x).replace('-','/') if '-' in str(x) else str(x))
		hotels['DATEOUT'] = hotels['DATEOUT'].apply(lambda x: str(x).replace('-','/') if '-' in str(x) else str(x))
		
		hotels['DATEIN'] = hotels['DATEIN'].apply(lambda x: fix_format(str(x)) if str(x).find('/') > 2 else str(x))
		hotels['DATEOUT'] = hotels['DATEOUT'].apply(lambda x: fix_format(str(x)) if str(x).find('/') > 2 else str(x))
		
		hotels['DATEIN'] = hotels['DATEIN'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)
		hotels['DATEOUT'] = hotels['DATEOUT'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)

		hotels['DATEIN'] = pd.to_datetime(hotels['DATEIN'])
		hotels['DATEIN'] = hotels['DATEIN'].dt.strftime('%m/%d/%Y')
		hotels['DATEOUT'] = pd.to_datetime(hotels['DATEOUT'])
		hotels['DATEOUT'] = hotels['DATEOUT'].dt.strftime('%m/%d/%Y')
		
		hotels['DATEIN'] = hotels['DATEIN'].apply(lambda x: x if str(x) != 'NaT' else '')
		hotels['DATEOUT'] = hotels['DATEOUT'].apply(lambda x: x if str(x) != 'NaT' else '')
		
		
		
		hotels.loc[ hotels['HOTELNAM'].isin(['','nan']), 'HOTELNAM'] = 'HOTEL MISCELLANEOUS'
		hotels.loc[ hotels['HOTELNAM'].isin(['','nan']), 'CHAINCOD'] = 'HZ'
		hotels.loc[ hotels['CHAINCOD'] == 'XX', 'CHAINCOD'] = ''
		
		# hotels.loc[ (~hotels['HOTELNAM'].isin(['','nan'])) & (hotels['CHAINCOD'].isin(hotel_chain_code)) , 'CHAINCOD'] = hotels['CHAINCOD']
		# hotels.loc[ (~hotels['HOTELNAM'].isin(['','nan'])) & (~hotels['CHAINCOD'].isin(hotel_chain_code)) , 'CHAINCOD'] = "IM"
		hotels['BOOKRATE'] = pd.to_numeric(hotels['BOOKRATE'])
		hotels.loc[ (hotels['BOOKRATE'] < 0) , 'HPLUSMIN'] = -1
		hotels.loc[ (hotels['BOOKRATE'] < 0) , 'TRANTYPE'] = 'C'
		hotels.loc[ (hotels['BOOKRATE'] > 0) , 'HPLUSMIN'] = 1


		for i, row in hotels.iterrows():
			# if str(row['HOTELNAM']) == '' or str(row['HOTELNAM']) == 'nan':
			# 	hotels.loc[i,'HOTELNAM'] = 'HOTEL MISCELLANEOUS'
			# 	hotels.loc[i,'CHAINCOD'] = 'HZ'
			if str(row['HOTELNAM']) != '' or str(row['HOTELNAM']) != 'nan':
				if row['CHAINCOD'] in hotel_chain_code:
					hotels.loc[i,'CHAINCOD'] = row['CHAINCOD']
				else:
					hotels.loc[i,'CHAINCOD'] = 'IM'
				for j,row2 in lut_hotels.iterrows():
					if str(row2['KEYWORD']) in row['HOTELNAM']:
						hotels.loc[i,'CHAINCOD'] = row2['CHAINCOD'] 

			# Populate UD54 if the country is poland
			if str(row['RECKEY'])[0:3] == '281' or str(row['RECKEY'])[0:3] == '282':
				ud54.append([row['RECKEY'],'HOTEL VOUCHER'])

			# if str(row['RECKEY'])[0:3] in fixCountriesDateList:
			# 	try:
			# 		hotels.loc[i,'DATEIN'] = datetime.datetime.strptime(row['DATEIN'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 		hotels.loc[i,'DATEOUT'] = datetime.datetime.strptime(row['DATEOUT'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 	except:
			# 		hotels.loc[i,'DATEIN'] = datetime.datetime.strptime(row['DATEIN'].replace('.','/'),'%m/%d/%Y').strftime('%m/%d/%Y')
			# 		hotels.loc[i,'DATEOUT'] = datetime.datetime.strptime(row['DATEOUT'].replace('.','/'),'%m/%d/%Y').strftime('%m/%d/%Y')
			# if row['CHAINCOD'] == 'XX':
			# 	hotels.loc[i, 'CHAINCOD'] = ''
			# if '-' in str(row['BOOKRATE']):
			# 	hotels.loc[i,'HPLUSMIN'] = '-1'
			# 	hotels.loc[i,'TRANTYPE'] = 'C'
			# else:
			# 	hotels.loc[i,'HPLUSMIN'] = '1'
			if row['METRO'] in airport_dict:
				site_info = airport_dict[row['METRO']]
				hotels.loc[i, 'HOTCOUNTRY'] = site_info[-1]
				hotels.loc[i, 'HOTCITY'] = site_info[0]
				if site_info[-1] == 'US' or site_info[-1] == 'CA':
					hotels.loc[i, 'HOTSTAT'] = site_info[1]
				else:
					hotels.loc[i, 'HOTSTAT'] = ''
			elif len(row['METRO']) != 3 and row['METRO'] != '   ':
				if row['HOTCITY'] == 'PORTLAND':
					hotels.loc[i, 'METRO'] = 'PDX'
					hotels.loc[i, 'HOTSTAT'] = 'OR'
					hotels.loc[i,'HOTCOUNTRY'] = 'US'
				elif row['HOTCITY'] == 'BEAVERTON' and row['HOTSTAT'] == 'OR':
					hotels.loc[i, 'METRO'] = 'PDX'
					hotels.loc[i, 'HOTSTAT'] = 'OR'
					hotels.loc[i,'HOTCOUNTRY'] = 'US'
				elif row['HOTCITY'] == 'TIGARD' and row['HOTSTAT'] == 'OR':
					hotels.loc[i, 'METRO'] = 'PDX'
					hotels.loc[i, 'HOTSTAT'] = 'OR'
					hotels.loc[i,'HOTCOUNTRY'] = 'US'
				else:
					for k, v in airport_dict.items():
						if v[0] == row['HOTCITY'] and v[-1] == row['HOTCOUNTRY']:
							hotels.loc[i, 'METRO'] = k
							if v[-1] == 'US' or v[-1] == 'CA':
								hotels.loc[i, 'HOTSTAT'] = v[1]
							else:
								hotels.loc[i, 'HOTSTAT'] = ''
						elif v[0] == row['HOTCITY'] and v[-1] == row['HOTSTAT']:
							hotels.loc[i, 'METRO'] = k
							hotels.loc[i, 'HOTCOUNTRY'] = row['HOTSTAT']
							hotels.loc[i, 'HOTSTAT'] = ''

			flag = False
			if str(row['MONEYTYPE']) in currencies:
				flag = True
			
			if not flag:
				if row['RECKEY'] in trips_acct:
					if str(trips_acct[row['RECKEY']]) in lut_currencies:
						hotels.loc[i,'MONEYTYPE'] = lut_currencies[str(trips_acct[row['RECKEY']])]
		
		for i, row in hotels.iterrows():
			try:
				if float(row['BOOKRATE']) > 400 and row['MONEYTYPE'] == 'EUR':
					error_report.append(['TEMP','TCHOTEL',row['RECKEY'],'Hotel Booked Rate Too High - Verify'])
			except:
				pass
			try:
				if float(row['BOOKRATE']) > 400 and row['MONEYTYPE'] == 'USD':
					error_report.append(['TEMP','TCHOTEL',row['RECKEY'],'Hotel Booked Rate Too High - Verify'])
			except:
				pass
			if row['HOTELNAM'] == 'NICHT GELIEFERT':
				dropped_hotels.append(row['RECKEY'])
				error_report.append(['TEMP','TCHOTEL',row['RECKEY'],'%s %s %s not a valid entry' % (row['HOTELNAM'],row['HOTCOUNTRY'],row['METRO'])])
			if row['METRO'] not in airport_dict:
				dropped_hotels.append(row['RECKEY'])
				error_report.append(['TEMP','TCHOTEL',row['RECKEY'],'%s %s %s not a valid entry' % (row['HOTELNAM'],row['HOTCOUNTRY'],row['METRO'])])
		
		print("You have crossed 1000 lines :)")

		hotels['HOTELNAM'] = hotels['HOTELNAM'].apply(lambda x: 'Unknown' if x == '' else x)
		hotels = hotels.sort_values(by=['RECKEY','NIGHTS'])

		hotels['D1'] = hotels.duplicated()
		hotels = hotels.drop_duplicates()

		hotels['D2'] = hotels.duplicated(['RECKEY','HOTELNAM','HOTCITY','DATEIN'], keep='first')

		hotels = hotels.drop_duplicates(['RECKEY','HOTELNAM','HOTCITY','DATEIN'], keep='last')
		hotels['HOTSTAT'] = hotels['HOTSTAT'].apply(lambda x: '' if str(x) == 'NONE' else x)
		hotels['METRO'] = hotels['METRO'].apply(lambda x: '' if str(x) == 'NONE' else x)
		for i,row in hotels.iterrows():
			if row['D1'] == True:
				deleted_records.append([row['RECKEY'],'Hotels:Complete Record Matched'])
			if row['D2'] == True:
				deleted_records.append([row['RECKEY'],'Hotels:RECKEY,HOTELNAM,HOTCITY & DATEIN Matched'])
		
		if 'D1' in hotels.columns:
			del hotels['D1']
		if 'D2' in hotels.columns:
			del hotels['D2']
			
	hotels.to_csv('{}\Tchotel.csv'.format(date),index=False)
	
	# This is the beginning of the code to process TCTRIPS.  Because it has to exchange information with TCLEGS and TCUDIDS before they are all
	# completely processed, it is not self-contained like the previous two.

	trips = d['TCTRIPS']
	if 'BKTOOL' in trips:
		trips['BKAGENT'] = trips['BKTOOL']
		trips.drop('BKTOOL', axis=1, inplace=True)
	
	# Here the data structures which store data to move in between tables are defined.

	de_UD39 = []
	UD27 = []
	UD409 = []
	UD409_trips_reckeys = []
	UD12Trips = []
	UD11Trips = []
	UD33Trips = []
	UD32Trips = []
	deleted_entries = {}
	online_booking = []
	travel_mode = []
	most_occured = 0
	month = []
	day = []

	if len(trips) != 0:
		# Because some of the date fields can have null values, these values are replaced with a sample date to allow processing.
		print("Welcom to Trips Section")
		trips.fillna('1/1/1900',inplace=True)
		# trips = trips.applymap(lambda x: str(x).upper().strip().replace(',','').replace('NAN',''))
		trips = trips.applymap(
			lambda x: str(x).upper().strip().replace(',', '').replace('NAN','') if x == "" or x == "nan" else str(
				x).upper().strip().replace(',', ''))
		# trips = trips.applymap(lambda x: unidecode(unicode(x, 'latin-1')))
		# This block of code implements the fix for float conversion on selected fields only.
		# The issue with values being off by orders of magnitude stemmed from a prior version of this fix that was applied to the whole DataFrame.
		
		for i, row in trips.iterrows():
			ms = row['INVDATE'].find('/')
			m = row['INVDATE'][:ms]
			month.append(m)
			ds = row['INVDATE'].rfind('/')
			da = row['INVDATE'][ms+1:ds]
			day.append(da)

		# print(month)
		cnt = Counter(month)
		# print(cnt)
		most_occured =  cnt.most_common(1)[0][0]
		# print(most_occured)

		# exit()
		trips['RECKEY'] = trips['RECKEY'].apply(lambda x: str(x).replace('.00','').replace('.0',''))
		trips['INVOICE'] = trips['INVOICE'].apply(lambda x: str(x).replace('.00','').replace('.0',''))
		trips['ACCT'] = trips['ACCT'].apply(lambda x: str(x).replace('.00','').replace('.0',''))
		trips['TICKET'] = trips['TICKET'].apply(lambda x: str(x).replace('.00','').replace('.0',''))
		trips['STNDCHG'] = trips['STNDCHG'].apply(lambda x: str(x).replace('01/01/1900','0').replace('1/1/1900','0'))
		trips['STNDCHG'] = trips['STNDCHG'].apply(lambda x: x.strip()).replace('', '0')
		trips['SAVINGCODE'] = ''

		trips['DEPDATE'] = trips['DEPDATE'].apply(lambda x: dateWithTripleForwardSlash(str(x),most_occured))
		trips['ARRDATE'] = trips['ARRDATE'].apply(lambda x: dateWithTripleForwardSlash(str(x),most_occured))

		trips['INVDATE'] = trips['INVDATE'].apply(lambda x: remInvalidDates(str(x)))
		trips['BOOKDATE'] = trips['BOOKDATE'].apply(lambda x: remInvalidDates(str(x)))
		trips['DEPDATE'] = trips['DEPDATE'].apply(lambda x: remInvalidDates(str(x)))
		trips['ARRDATE'] = trips['ARRDATE'].apply(lambda x: remInvalidDates(str(x)))

		# trips['INVDATE'] = trips['INVDATE'].apply(lambda x: convert_date(x) if '-' in str(x) else x)
		trips['BOOKDATE'] = trips['BOOKDATE'].apply(lambda x: convert_date(x) if '-' in str(x) else x)
		trips['DEPDATE'] = trips['DEPDATE'].apply(lambda x: convert_date(x) if '-' in str(x) else x)
		trips['ARRDATE'] = trips['ARRDATE'].apply(lambda x: convert_date(x) if '-' in str(x) else x)
		
		trips['INVDATE'] = trips['INVDATE'].apply(lambda x: str(x).replace('-','/') if '-' in str(x) else str(x))
		trips['BOOKDATE'] = trips['BOOKDATE'].apply(lambda x: str(x).replace('-','/') if '-' in str(x) else str(x))
		trips['DEPDATE'] = trips['DEPDATE'].apply(lambda x: str(x).replace('-','/') if '-' in str(x) else str(x))
		trips['ARRDATE'] = trips['ARRDATE'].apply(lambda x: str(x).replace('-','/') if '-' in str(x) else str(x))
		
		trips['INVDATE'] = trips['INVDATE'].apply(lambda x: fix_format(str(x)) if str(x).find('/') > 2 else str(x))
		trips['BOOKDATE'] = trips['BOOKDATE'].apply(lambda x: fix_format(str(x)) if str(x).find('/') > 2 else str(x))
		trips['DEPDATE'] = trips['DEPDATE'].apply(lambda x: fix_format(str(x)) if str(x).find('/') > 2 else str(x))
		trips['ARRDATE'] = trips['ARRDATE'].apply(lambda x: fix_format(str(x)) if str(x).find('/') > 2 else str(x))
		
		trips['INVDATE'] = trips['INVDATE'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)
		trips['BOOKDATE'] = trips['BOOKDATE'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)
		trips['DEPDATE'] = trips['DEPDATE'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)
		trips['ARRDATE'] = trips['ARRDATE'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)

		# trips['INVDATE'] = pd.to_datetime(trips['INVDATE'])
		# trips['INVDATE'] = trips['INVDATE'].dt.strftime('%m/%d/%Y')
		# trips['BOOKDATE'] = pd.to_datetime(trips['BOOKDATE'])
		# trips['BOOKDATE'] = trips['BOOKDATE'].dt.strftime('%m/%d/%Y')
		#
		# trips['DEPDATE'] = pd.to_datetime(trips['DEPDATE'])
		# trips['DEPDATE'] = trips['DEPDATE'].dt.strftime('%m/%d/%Y')
		# trips['ARRDATE'] = pd.to_datetime(trips['ARRDATE'])
		# trips['ARRDATE'] = trips['ARRDATE'].dt.strftime('%m/%d/%Y')
		#
		trips['INVDATE'] = trips['INVDATE'].apply(lambda x: x if str(x) != 'NaT' else '')
		trips['BOOKDATE'] = trips['BOOKDATE'].apply(lambda x: x if str(x) != 'NaT' else '')
		trips['DEPDATE'] = trips['DEPDATE'].apply(lambda x: x if str(x) != 'NaT' else '')
		trips['ARRDATE'] = trips['ARRDATE'].apply(lambda x: x if str(x) != 'NaT' else '')
		
		# legDepDate dict is populating with Legs RDEPDATE and Legs RARRDATE. Duplication will be removed but the 
		# old RDEPDATE date will be preserved and all duplicate RECEKS with latest date will be removed. 
		legDepDate = pd.Series(d['TCLEGS']['RDEPDATE'].values,index=d['TCLEGS'].RECKEY)
		grouped = legDepDate.groupby(level=0)
		legDepDate = grouped.first()
		legDepDate = legDepDate.to_dict()

		# Dictionary is populating with data without duplication with Latest dates against each reckey and all
		# duplicate RECKEYS with old dates in RARRDATE will be removed.
		legArrDate = pd.Series(d['TCLEGS']['RARRDATE'].values,index=d['TCLEGS'].RECKEY).to_dict()

		hotelDepDate = pd.Series(hotels['DATEIN'].values,index=hotels.RECKEY).to_dict()
		carDepDate = pd.Series(cars['RENTDATE'].values,index=cars.RECKEY).to_dict()

		hotelArrDate = pd.Series(hotels['DATEOUT'].values,index=hotels.RECKEY).to_dict()
		carArrDate = pd.Series(cars['DATEBACK'].values,index=cars.RECKEY).to_dict()
		
		
		
		trips.loc[ trips['BREAK1'].str.isdigit() == True, 'BREAK1'] = ''			
		trips.loc[ trips['BREAK2'].str.isdigit() == True, 'BREAK2'] = ''			
		trips.loc[ trips['BREAK3'].str.isdigit() == True, 'BREAK3'] = ''			
		trips.loc[ trips['VALCARR'] == 'ZE', 'VALCARR'] = 'ZZ'			
		trips.loc[ trips['BREAK2'] == '0', 'BREAK2'] = ''
		
		trips['AIRCHG'] = pd.to_numeric(trips['AIRCHG'])
		trips.loc[ trips['AIRCHG'] < 0 , 'PLUSMIN'] = -1
		# update PLUSMIN to C where AIRCHG < 0
		trips.loc[ trips['AIRCHG'] < 0 , 'TRANTYPE'] = 'C'
		# update PLUSMIN to 1 where AIRCHG > 0
		trips.loc[ trips['AIRCHG'] > 0 , 'PLUSMIN'] = 1

		trips.loc[ trips['VALCARR'] == 'ZZ', ['VALCARRMOD', 'STNDCHG', 'MKTFARE', 'REASCODE', 'OFFRDCHG', 'BASEFARE', 'FARETAX', 'SVCFEE', 'TAX1', 'TAX2', 'TAX3', 'TAX4'] ] = ''
		# trips.loc[ trips['VALCARR'] == 'ZZ', 'AIRCHG' ] = 0 # we need to set AIRCHG to 0 for ZZ trips
		
		
				

		for i, row in trips.iterrows():
			if row['ACCT'] in remove_reascode_acct:
				if str(row['REASCODE']) == 'RS':
					trips.drop(i,inplace=False)
			# if the record is 'ZZ' then Hotels and cars dates will be assigned to trip's dates else legs dates will be used
			if row['VALCARR'] == 'ZZ':
				if str(row['DEPDATE']) == '00/00/0000' or str(row['DEPDATE']) == '' or str(row['DEPDATE']) == 'nan' or str(row['DEPDATE']) == 'NaT':
					if row['RECKEY'] in hotelDepDate: 
						trips.loc[i,'DEPDATE'] = hotelDepDate[row['RECKEY']]
					elif row['RECKEY'] in carDepDate:
						trips.loc[i,'DEPDATE'] = carDepDate[row['RECKEY']]
				if str(row['ARRDATE']) == '00/00/0000' or str(row['ARRDATE']) == '' or str(row['ARRDATE']) == 'nan' or str(row['ARRDATE']) == 'NaT':
					if row['RECKEY'] in hotelArrDate: 
						trips.loc[i,'ARRDATE'] = hotelArrDate[row['RECKEY']]
					elif row['RECKEY'] in carArrDate:
						trips.loc[i,'ARRDATE'] = carArrDate[row['RECKEY']]
			else:
				if str(row['DEPDATE']) == '00/00/0000' or str(row['DEPDATE']) == '' or str(row['DEPDATE']) == 'nan' or str(row['DEPDATE']) == 'NaT':
					if row['RECKEY'] in legDepDate:
						trips.loc[i,'DEPDATE'] = legDepDate[row['RECKEY']]
				if str(row['ARRDATE']) == '00/00/0000' or str(row['ARRDATE']) == '' or str(row['ARRDATE']) == 'nan' or str(row['ARRDATE']) == 'NaT':
					if row['RECKEY'] in legArrDate:
						trips.loc[i,'ARRDATE'] = legArrDate[row['RECKEY']]
			# [Fs] fix date format
			
			# if str(row['RECKEY'])[0:3] in fixCountriesDateList:
			# 	try:
			# 		trips.loc[i,'INVDATE'] = datetime.datetime.strptime(row['INVDATE'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 	except:
			# 		continue
			# 	try:
			# 		trips.loc[i,'BOOKDATE'] = datetime.datetime.strptime(row['BOOKDATE'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 	except:
			# 		continue
			# 	try:
			# 		trips.loc[i,'DEPDATE'] = datetime.datetime.strptime(row['DEPDATE'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 	except:
			# 		continue
			# 	try:
			# 		trips.loc[i,'ARRDATE'] = datetime.datetime.strptime(row['ARRDATE'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 	except:
			# 		continue

			date_format = "%m/%d/%Y"
			if row['BOOKDATE'] != '' and row['DEPDATE'] != '':
				try:
					a = datetime.datetime.strptime(row['BOOKDATE'], date_format)
					b = datetime.datetime.strptime(row['DEPDATE'], date_format)
					delta = b - a
					if delta.days < 0:
						trips.loc[i,'BOOKDATE'] = row['DEPDATE']
				except:
						continue

			# This code strips out certain values that are always invalid, including break values without numbers and VALCARR 'ZE'.


			# FouadBook = pd.Series(trips['BOOKDATE'].values, index=trips.RECKEY).to_dict()
			# print FouadBook['2040527']
			# if str(row['BREAK1']).isdigit() == True:
			# 	# print('break1',row['RECKEY'])
			# 	trips.loc[i, 'BREAK1'] = ''
			# if str(row['BREAK2']).isdigit() == True:
			# 	trips.loc[i, 'BREAK2'] = ''
			# if str(row['BREAK3']).isdigit() == True:
			# 	trips.loc[i, 'BREAK3'] = ''
			# if row['VALCARR'] == 'ZE':
			# 	trips.loc[i, 'VALCARR'] = 'ZZ'
			# if str(row['BREAK2']) == '0':
			# 	trips.loc[i, 'BREAK2'] = ''

			# As before, sets plusmin value depending on the presence of a '-' in the field.

			# if '-' in str(row['AIRCHG']):
			# 	trips.loc[i,'PLUSMIN'] = '-1'
			# 	trips.loc[i,'TRANTYPE'] = 'C'
			# else:
			# 	trips.loc[i,'PLUSMIN'] = '1'


			# FouadBook = pd.Series(trips['BOOKDATE'].values, index=trips.RECKEY).to_dict()
			# print FouadBook['2040527']
			# # time.sleep(12)

		# trips['INVDATE'] = trips['INVDATE'].apply(lambda x: fix_date_format(x,most_occured) if x[:2] != most_occured else x)
		# trips['BOOKDATE'] = trips['BOOKDATE'].apply(lambda x: fix_date_format(x,most_occured) if x[:2] != most_occured else x)
		trips = trips.applymap(lambda x: str(x).replace('01/01/1900','').replace('1/1/1900',''))

		# The set of rules in this loop does some country-specific break fixes, writes online booking values to array, and other generic fixes.
		
		trips.loc[trips['DOMINTL'].isin(['','nan']), 'DOMINTL'] = "D"
		trips.loc[(trips['VALCARR'] == 'ZZ') & (trips['AIRCHG'].isin(['',0,'nan'])), 'VALCARRMOD'] = ''
		trips.loc[(trips['RECKEY'][0:3] == '281') & (trips['VALCARR'].isin(['PKPI','PKPA'])), 'VALCARR'] = '2P'
		trips.loc[(trips['VALCARR'] == '') & (trips['VALCARRMOD'] == 'R'), 'VALCARR'] = '1R'
		trips.loc[(trips['VALCARR'] == '') & (trips['VALCARRMOD'] == 'A'), 'VALCARR'] = 'YY'
		# Update valcarr for SWISS RAIL
		trips.loc[(trips['RECKEY'][0:3] == '145') & (trips['VALCARRMOD'] == 'R'), 'VALCARR'] = 'SB'
		trips.loc[(trips['RECKEY'][0:3] == '122') & (trips['BREAK1'] == 'BE'), 'BREAK2'] = trips['BREAK1']
		trips.loc[(trips['RECKEY'] == '161') | (trips['RECKEY'] == '173') | (trips['RECKEY'] == '269')  | (trips['RECKEY'] == '299'), 'BREAK2'] = trips['BREAK1']
		# trips.loc[(trips['RECKEY'][3:5] == '05') & (trips['RECKEY'][3:5] != '') , 'BREAK3'] = ''
		# trips.loc[(trips['RECKEY'][3:5] == '08'), 'BREAK3'] = ''
		trips.loc[(trips['RECKEY'][0:5] == '11505') & (trips['BREAK3'] != ''), 'TRAVELERID'] = trips['BREAK3']
		# print(account_dict)
		# trips.loc[(trips['ACCT'].replace('.0','').isin(account_dict)), 'BREAK1'] = account_dict[0]
		
		trips.loc[(trips['RECLOC'] == 'N/A'), 'RECLOC'] = ''
		trips.loc[(trips['DOMINTL'] == 'T'), 'DOMINTL'] = 'I'
		trips.loc[trips['BKAGENT'] == '', 'BKAGENT'] = 'AGENT'
		trips.loc[trips['VALCARR'] == 'JAPANRAIL', 'JAPANRAIL'] = 'JR'
		trips.loc[trips['VALCARR'] == 'BAHN', 'JAPANRAIL'] = 'DB'


		for i, row in trips.iterrows():
			
			flag = False
			if str(row['MONEYTYPE']) in currencies:
				flag = True
			
			if not flag:
				if row['RECKEY'] in trips_acct:
					# if trips_acct[row['RECKEY']] in lut_currencies:
					if str(row['ACCT']) in lut_currencies:	
						trips.loc[i,'MONEYTYPE'] = lut_currencies[str(row['ACCT'])]

			if row['BOOKINGNBR'] != '':
				UD409.append([row['RECKEY'],row['BOOKINGNBR']])
				UD409_trips_reckeys.append(row['RECKEY'])
			if row['VALCARRMOD'] == 'A':
				if row['RECKEY'] not in hotels['RECKEY']:
					UD12Trips.append(row['RECKEY'])
			if row['VALCARR'] == 'ZZ':
				UD11Trips.append(row['RECKEY'])
			# if row['DOMINTL'] == '' or str(row['DOMINTL']) == 'nan':
			# 	row['DOMINTL'] = 'D'
			# if row['VALCARR'] == 'ZZ' and (row['AIRCHG'] == 0 or row['AIRCHG'] == '' or str(row['AIRCHG']) == 'nan'):
			# 	row['VALCARRMOD'] = ''

			# if country is Poland and TRIPS.VALCARR contains 'PKPI' or 'PKPA' then '2P' will be populated in TRIPS.VALCARR
			# if str(row['RECKEY'])[0:3] == '281' and (str(row['VALCARR']) == 'PKPI' or str(row['VALCARR']) == 'PKPA'):
			# 	trips.loc[i,'VALCARR'] = '2P'

			# if row['BREAK2'] != '':
			# 	UD32Trips.append([row['RECKEY'],row['BREAK2']])
			# if (str(row['VALCARR']) == '' or str(row['VALCARR']) == 'nan') and str(row['VALCARRMOD']) == 'R':
			# 	trips.loc[i,'VALCARR'] = '1R'
			
			# Update valcarr for SWISS RAIL			
			if str(row['RECKEY'])[:3] == '145' and row['VALCARRMOD'] == 'R':
				trips.loc[i,'VALCARR'] = 'SB'
			if type(row['STNDCHG']) == float:
				if float(row['STNDCHG']) != 0:
					UD33Trips.append([row['RECKEY'], row['STNDCHG']])
			if len(str(row['BREAK2']).strip()) == 3 and str(row['RECKEY'])[0:3] == '273':
				trips.loc[i,'BREAK3'] = row['BREAK2']
				trips.loc[i,'BREAK2'] = row['BREAK1']
			if str(row['RECKEY'])[0:3] == '158' and row['BREAK3'] != '':
				UD27.append([row['RECKEY'],row['BREAK3']])
			# if str(row['RECKEY'])[0:3] == '158':
			# 	trips.loc[i, 'BREAK2'] = trips.loc[i, 'BREAK1']
			# if str(row['RECKEY'])[0:3] == '159' and row['BREAK1'] != '':
			# 	de_UD39.append([row['RECKEY'],row['BREAK1']])
			# if str(row['RECKEY'])[0:3] == '159' and row['BREAK2'] != '':
			# 	UD27.append([row['RECKEY'],row['BREAK2']])
			# if str(row['RECKEY'])[0:3] == '122' and row['BREAK2'] != '':
			# 	UD27.append([row['RECKEY'],row['BREAK2']])
			# if str(row['RECKEY'])[0:3] == '159' or str(row['RECKEY'])[0:3] == '170' or str(row['RECKEY'])[0:3] == '286':
			# if str(row['RECKEY'])[0:3] == '170' or str(row['RECKEY'])[0:3] == '286':
			# 	trips.loc[i, 'BREAK2'] = trips.loc[i, 'BREAK1']
			# if str(row['RECKEY'])[0:3] == '122' and row['BREAK1'] != 'BE':
			# 	trips.loc[i, 'BREAK2'] = trips.loc[i, 'BREAK1']
			# if str(row['RECKEY'])[0:3] == '161' or str(row['RECKEY'])[0:3] == '173' or str(row['RECKEY'])[0:3] == '269' or str(row['RECKEY'])[0:3] == '299':
			# 	trips.loc[i, 'BREAK2'] = trips.loc[i, 'BREAK1']
			# if str(row['RECKEY'])[0:5] == '11505' and row['BREAK3'] != '':
			# 	trips.loc[i,'TRAVELERID'] = row['BREAK3']
			if str(row['ACCT']).replace('.0','') in account_dict:
				trips.loc[i, 'BREAK1'] = account_dict[str(row['ACCT']).replace('.0','')][0]
			# if row['RECLOC'] == 'N/A':
			# 	trips.loc[i, 'RECLOC'] = ''
			# if row['DOMINTL'] == 'T':
			# 	trips.loc[i, 'DOMINTL'] = 'I'
			# if row['VALCARR'] == '':
			# 	if row['VALCARRMOD'] == 'R':
			# 		trips.loc[i, 'VALCARR'] = '1R'
			# 	elif row['VALCARRMOD'] == 'A':
			# 		trips.loc[i, 'VALCARR'] = 'YY'
			if str(row['RECKEY'])[3:5] == '08':
				trips.loc[i, 'BREAK3'] = ''
			if str(row['RECKEY'])[3:5] == '05':
				trips.loc[i, 'BREAK3'] = ''
			if row['TKAGENT'] == 'BNEONLINE' or row['TKAGENT'] == 'ONLINE':
				online_booking.append(row['RECKEY'])
			if row['BKAGENT'] == 'ONLINE' or row['AGENTID'] == 'ONLINE':
				online_booking.append(row['RECKEY'])
			
			if str(row['BKAGENT']).find('ONLINE') != -1:
				trips.loc[i,'BKAGENT'] = 'ONLINE'
				online_booking.append(row['RECKEY'])
			else:
				trips.loc[i,'BKAGENT'] = 'AGENT'

			# if row['BKAGENT'] == '':
			# 	trips.loc[i,'BKAGENT'] = 'AGENT'	
			# if row['VALCARRMOD'] == '' and row['VALCARR'] != 'ZZ':
			# 	travel_mode.append(str(row['RECKEY']))
			# if row['VALCARR'] == 'JAPANRAIL':
			# 	trips.loc[i,'VALCARR'] = 'JR'
			# if row['VALCARR'] == 'BAHN':
			# 	trips.loc[i,'VALCARR'] = 'DB'

			# if row['VALCARRMOD'] == '' or str(row['VALCARRMOD']) == 'nan':
			# 	trips.loc[i, 'VALCARR'] = 'ZZ'
			# 	trips.loc[i, 'STNDCHG'] = ''
			# 	trips.loc[i, 'MKTFARE'] = ''
			# 	trips.loc[i, 'REASCODE'] = ''
			# 	trips.loc[i, 'OFFRDCHG'] = ''
			# 	trips.loc[i, 'AIRCHG'] = ''
			#
			#
			# if row['VALCARR'] == 'ZZ':
			# 	trips.loc[i, 'STNDCHG'] = ''
			# 	trips.loc[i, 'VALCARRMOD'] = ''
			# 	trips.loc[i, 'MKTFARE'] = ''
			# 	trips.loc[i, 'REASCODE'] = ''
			# 	trips.loc[i, 'OFFRDCHG'] = ''
			# 	trips.loc[i, 'AIRCHG'] = ''
			# 	trips.loc[i, 'BASEFARE'] = ''
			# 	trips.loc[i, 'FARETAX'] = ''
			# 	trips.loc[i, 'SVCFEE'] = ''
			# 	trips.loc[i, 'TAX1'] = ''
			# 	trips.loc[i, 'TAX2'] = ''
			# 	trips.loc[i, 'TAX3'] = ''
			# 	trips.loc[i, 'TAX4'] = ''
			# 	deleted_entries[row['RECKEY']] = 'VALCARR ZZ - NOT A VALID TRIP'

			if row['BREAK1'] in currency_dict:
				if row['MONEYTYPE'] != currency_dict[row['BREAK1']]:
					trips.loc[i,'MONEYTYPE'] = currency_dict[row['BREAK1']]
			# If TCACCTS has Merck PH ,MY ,SG account names then update the REASCODE .
			
			if _flag != 0:	
				
				AL = ['101','101.0','LF','102','102.0','GV','103','103.0','RW','104','104.0','SP','901','901.0','FS','906','906.0','CP']
				AR = ['105','105.0','CT','902','902.0','FE']
				AX = ['106','106.0','RT','107','107.0','RE']
				AP = ['108','108.0','903','903.0','CS','908','908.0','FP']
				AH = ['904','904.0','CM','905','905.0','FM']
				AT = ['907','907.0','FT']
				AZ = ['909','909.0','FR']
				
				if str(row['REASCODE']) in AL:
					trips.loc[i, 'REASCODE'] = 'AL'
				if str(row['REASCODE']) in AR:
					trips.loc[i, 'REASCODE'] = 'AR'
				if str(row['REASCODE']) in AX:
					trips.loc[i, 'REASCODE'] = 'AX'
				if str(row['REASCODE']) in AP:
					trips.loc[i, 'REASCODE'] = 'AP'
				if str(row['REASCODE']) in AH:
					trips.loc[i, 'REASCODE'] = 'AH'
				if str(row['REASCODE']) in AT:
					trips.loc[i, 'REASCODE'] = 'AT'
				if str(row['REASCODE']) in AZ:
					trips.loc[i, 'REASCODE'] = 'AZ'

		# validate reascode
		trips['REASCODE'] = trips['REASCODE'].apply(lambda x: validateReascode(str(x)))
	# Notice that the code switches to legs without finishing writing trips to csv.
	# More arrays are defined, and the first segment of legs code does the same thing as what has gone on before.
	valcarr_mode = {}
	airlines = []
	new_airlines = {}
	print(d['TCLEGS'])
	legs = d['TCLEGS']
	# exit()
	if len(legs) != 0:
		print("Welcome to Legs Section")
		legs.fillna('1/1/1900',inplace=True)
		legs = legs.applymap(lambda x: str(x).upper().strip().replace(',','').replace(';','').replace('NAN','1/1/1900'))
		# legs = legs.applymap(lambda x: unidecode(unicode(x, 'latin-1')))

		legs['RDEPDATE'] = legs['RDEPDATE'].apply(lambda x: dateWithTripleForwardSlash(str(x),most_occured))
		legs['RARRDATE'] = legs['RARRDATE'].apply(lambda x: dateWithTripleForwardSlash(str(x),most_occured))

		legs['RDEPDATE'] = legs['RDEPDATE'].apply(lambda x: remInvalidDates(str(x)))
		legs['RARRDATE'] = legs['RARRDATE'].apply(lambda x: remInvalidDates(str(x)))
		
		legs['RDEPDATE'] = legs['RDEPDATE'].apply(lambda x: convert_date(x) if '-' in str(x) else x)
		legs['RARRDATE'] = legs['RARRDATE'].apply(lambda x: convert_date(x) if '-' in str(x) else x)

		legs['RDEPDATE'] = legs['RDEPDATE'].apply(lambda x: str(x).replace('-','/') if '-' in str(x) else str(x))
		legs['RARRDATE'] = legs['RARRDATE'].apply(lambda x: str(x).replace('-','/') if '-' in str(x) else str(x))

		legs['RDEPDATE'] = legs['RDEPDATE'].apply(lambda x: fix_format(str(x)) if str(x).find('/') > 2 else str(x))
		legs['RARRDATE'] = legs['RARRDATE'].apply(lambda x: fix_format(str(x)) if str(x).find('/') > 2 else str(x))
		
		legs['RDEPDATE'] = legs['RDEPDATE'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)
		legs['RARRDATE'] = legs['RARRDATE'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)

		legs['RDEPDATE'] = pd.to_datetime(legs['RDEPDATE'])
		legs['RDEPDATE'] = legs['RDEPDATE'].dt.strftime('%m/%d/%Y')
		legs['RDEPDATE'] = legs['RDEPDATE'].apply(lambda x: x if str(x) != 'NaT' else '')

		legs['RARRDATE'] = pd.to_datetime(legs['RARRDATE'])
		legs['RARRDATE'] = legs['RARRDATE'].dt.strftime('%m/%d/%Y')
		legs['RARRDATE'] = legs['RARRDATE'].apply(lambda x: x if str(x) != 'NaT' else '')
		# [fs] to fix date format
		# for i, row in legs.iterrows():
			# if str(row['RECKEY'])[0:3] in fixCountriesDateList:
			# 	try:
			# 		legs.loc[i,'RDEPDATE'] = datetime.datetime.strptime(row['RDEPDATE'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 	except:
			# 		continue
			# 	try:
			# 		legs.loc[i,'RARRDATE'] = datetime.datetime.strptime(row['RARRDATE'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
			# 	except:
			# 		continue

		legs = legs.applymap(lambda x: str(x).replace('01/01/1900','').replace('1/1/1900',''))

		invalidTime = ['00','0']

		legs['ARRTIME'] = legs['ARRTIME'].apply(lambda x: fix_time(x) if len(str(x)) == 3 else x)
		legs['ARRTIME'] = legs['ARRTIME'].apply(lambda x: str(x).replace(':','') if str(x).find(':') != -1 else str(x))
		legs['ARRTIME'] = legs['ARRTIME'].apply(lambda x: str(x) if x not in invalidTime else '')

		legs['DEPTIME'] = legs['DEPTIME'].apply(lambda x: fix_time(x) if len(str(x)) == 3 else x)
		legs['DEPTIME'] = legs['DEPTIME'].apply(lambda x: str(x).replace(':','') if str(x).find(':') != -1 else str(x))
		legs['DEPTIME'] = legs['DEPTIME'].apply(lambda x: str(x) if x not in invalidTime else '')
		
		legs.loc[legs['ARRTIME'].isin(['','000','0000']), 'ARRTIME'] = 1200
		legs.loc[legs['DEPTIME'].isin(['','000','0000']), 'DEPTIME'] = 700
		legs.loc[legs['AIRLINE'] == 'JAPANRAIL','AIRLINE'] = 'JR'
		legs['ACTFARE'] = pd.to_numeric(legs['ACTFARE'])
		legs.loc[legs['ACTFARE'] < 0,'RPLUSMIN'] = -1
		legs.loc[legs['ACTFARE'] > 0,'RPLUSMIN'] = 1

		for i, row in legs.iterrows():
			# if str(row['ARRTIME']) == '' or str(row['ARRTIME']) == '000' or str(row['ARRTIME']) == '0000':
			# 	legs.loc[i,'ARRTIME'] = 1200
			# if str(row['DEPTIME']) == '' or str(row['DEPTIME']) == '000' or str(row['DEPTIME']) == '0000':
			# 	legs.loc[i,'DEPTIME'] = 700
			if i < len(legs)-1:
				if row['RECKEY'] == legs.loc[i+1,'RECKEY']:
					if row['RDEPDATE'] == legs.loc[i+1,'RDEPDATE'] and row['RARRDATE'] == legs.loc[i+1,'RARRDATE']:
						if (legs.loc[i+1,'DEPTIME'] == '' or legs.loc[i+1,'DEPTIME'] == '000' or legs.loc[i+1,'DEPTIME'] == '0000') and (row['DEPTIME'] == 700 or row['DEPTIME'] == 1300) and (legs.loc[i+1,'ARRTIME'] == '' or legs.loc[i+1,'ARRTIME'] == '000' or legs.loc[i+1,'ARRTIME'] == '0000') and (row['ARRTIME'] == 1200 or row['ARRTIME'] == 1800): 
							legs.loc[i+1,'DEPTIME'] =  row['ARRTIME'] + 100
							legs.loc[i+1,'ARRTIME'] =  row['ARRTIME'] + 600
							if legs.loc[i+1,'ARRTIME'] == 2400:
								legs.loc[i+1,'ARRTIME'] = 2359
			
			dfh = str(row['DEPTIME'])[:-2+len(str(row['DEPTIME']))]
			dfh = '00' if len(dfh) == 0 else dfh
			dsh = str(row['DEPTIME'])[-2:]

			afh = str(row['ARRTIME'])[:-2+len(str(row['ARRTIME']))]
			afh = '00' if len(afh) == 0 else afh
			ash = str(row['ARRTIME'])[-2:]

			legs.loc[i,'DEPTIME'] = dfh + ':' + dsh
			legs.loc[i,'ARRTIME'] = afh + ':' + ash 

			# Rule to convert ORIGIN and DESTINAT of length 5 to 3 by getting last 3 char of str
			if row['RECKEY'][0:3] == '178':
				legsOriginLen = len(row['ORIGIN'])
				legDestinatLen = len(row['DESTINAT']) 
				if legsOriginLen == 5:
					legs.loc[i,'ORIGIN'] = row['ORIGIN'][2:5] 
				if legDestinatLen == 5:
					legs.loc[i,'DESTINAT'] = row['DESTINAT'][2:5]

			if row['ORIGIN'] == 'XXX' or row['DESTINAT'] == 'XXX':
				deleted_entries[row['RECKEY']] = 'XXX/ZZZ - NOT A VALID TRIP'
			if row['ORIGIN'] == '' and row['DESTINAT'] == '':
				deleted_entries[row['RECKEY']] = 'ORIGIN AND DESTINATION BLANK'
			if row['ORIGIN'].isdigit() == True and row['DESTINAT'].isdigit() == True:
				deleted_entries[row['RECKEY']] = 'NO VALID INFORMATION IN ORIG/DEST'
			if str(row['RECKEY']) in travel_mode:
				valcarr_mode[str(row['RECKEY'])] = row['MODE']
			if row['AIRLINE'] == '' or row['AIRLINE'] == ' ':
				airlines.append(str(row['RECKEY']))
			# if row['AIRLINE'] == 'JAPANRAIL':
			# 	legs.loc[i,'AIRLINE'] = 'JR'
			# if '-' in str(row['ACTFARE']):
			# 	cars.loc[i,'RPLUSMIN'] = '-1'
			# else:
			# 	cars.loc[i,'RPLUSMIN'] = '1'
			if row['ORIGIN'] in station_dict:
				legs.loc[i,'ORIGIN'] = station_dict[row['ORIGIN']]
			if row['DESTINAT'] in station_dict:
				legs.loc[i,'DESTINAT'] = station_dict[row['DESTINAT']]
		# print(airlines)
		legs = legs.applymap(lambda x: str(x).replace('01/01/1900','').replace('1/1/1900',''))
		for i, row in legs.iterrows():
			if row['RECKEY'] in deleted_entries:
				error_report.append(['TEMP','TCLEGS',row['RECKEY'][0:5], deleted_entries[row['RECKEY']]])
		
		for i in deleted_entries:
			legs = legs[legs.RECKEY != i]	
	# Accounts code is straight forward, though you should notice that the acctName dict contains the standard values for ACCTNAME field
	# based on its corresponding account number.

	accounts = d['TCACCTS']
	print("Welcom to Accounts Section")
	acct_to_fix = {"180882":"882","180883":"883","180884":"884","180885":"885","182458":"2458"}

	skechers_acct =[]
	if len(accounts) != 0:
		accounts = accounts.drop_duplicates()
		# accounts = accounts.applymap(lambda x: str(x).upper().strip().replace(',','').replace('NAN',''))
		accounts = accounts.applymap(lambda x: str(x).upper().strip().replace(',', '').replace('NAN','') if x == "" or x == "nan" else str(x).upper().strip().replace(',', ''))
		accounts['ACCT'] = accounts['ACCT'].apply(lambda x: acct_to_fix[str(x)] if str(x) in acct_to_fix else x)
		for i, row in accounts.iterrows():
			if str(row['ACCT']).replace('.0','').lstrip('0') in acctName_dict:
				accounts.loc[i,'ACCTNAME'] = acctName_dict[str(row['ACCT']).replace('.0','').lstrip('0')]
				accounts.loc[i,'ACCT'] = str(row['ACCT']).replace('.0','').lstrip('0')
				# The following lines of code to find out the SKECHERS account numbers.
				if 'SKECHERS' in acctName_dict[str(row['ACCT']).replace('.0','').lstrip('0')]:
					skechers_acct.append(str(row['ACCT']).replace('.0','').lstrip('0'))
			else:
				accounts.drop(i,inplace=True)

	accounts.to_csv('{}\Tcaccts.csv'.format(date),index=False)
	print(accounts)

	# Services is straight forward, other than the date, and that it must wait for svcfees files, which may appear, to be processed too.
	services = d['TCSERVICES']
	print("Welcom to TCSERVICES Section")
	services['SVCCODE'] = 'TSF'
	if len(services) != 0:

		services['TRANDATE'] = services['TRANDATE'].apply(lambda x: remInvalidDates(str(x)))

		services['TRANDATE'] = services['TRANDATE'].apply(lambda x: convert_date(x) if '-' in str(x) else x)

		services['TRANDATE'] = services['TRANDATE'].apply(lambda x: fix_invalid_date(x) if len(x) < 10 else x)
	
		services['TRANDATE'] = pd.to_datetime(services['TRANDATE'])
		services['TRANDATE'] = services['TRANDATE'].dt.strftime('%m/%d/%Y')
		services['TRANDATE'] = services['TRANDATE'].apply(lambda x: x if str(x) != 'NaT' else '')
		services["SVCAMT"] = pd.to_numeric(services["SVCAMT"])
		services.loc[services['SVCAMT'] < 0, 'SFTRANTYPE'] = 'C'
		
		for i, row in services.iterrows():
			flag = False
			if str(row['MONEYTYPE']) in currencies:
				flag = True
			
			if not flag:
				if row['RECKEY'] in trips_acct:
					if str(trips_acct[row['RECKEY']]) in lut_currencies:
						services.loc[i,'MONEYTYPE'] = lut_currencies[str(trips_acct[row['RECKEY']])]
			# [fs] to fix date format
			# if str(row['RECKEY'])[0:3] in fixCountriesDateList:

				# try:
				# 	services.loc[i,'TRANDATE'] = datetime.datetime.strptime(row['TRANDATE'].replace('.','/'),'%d/%m/%Y').strftime('%m/%d/%Y')
				# except:
				# 	continue
					# services.loc[i,'TRANDATE'] = datetime.datetime.strptime(row['TRANDATE'].replace('.','/'),'%m/%d/%Y').strftime('%m/%d/%Y')
			
			# if '-' in str(row['SVCAMT']):
			# 	services.loc[i,'SFTRANTYPE'] = 'C'
			
			if str(row['RECKEY'])[0:3] in _country_number_dict:
				if str(row['SFTRANTYPE']) == '' or str(row['SFTRANTYPE']) == 'nan':
					services.loc[i,'SFTRANTYPE'] = currency_dict[_country_number_dict[str(row['RECKEY'])[0:3]]]
	# This code converts standard TCSVCFEES files into TCSERVICES, adds them to services, then writes the combined data to csv.

	svcfees = d['TCSVCFEES']
	print("Welcom to TCSVCFEES Section")

	if len(svcfees) != 0:
		svcfees['SVCCODE'] = 'TSF'
		svcfees['VENDORCODE'] = ''
		svcfees['SFCREDCARD'] = ''
		new_services = svcfees[['RECKEY','SVCCODE','DESCRIPT','SVCFEE','MONEYTYPE','TRANDATE','VENDORCODE','TAX1','TAX2','TAX3','TAX4','TRANTYPE','SFCREDCARD','CARDNUM','MCO']]
		new_services = new_services.rename(columns={'DESCRIPT':'SVCDESC','SVCFEE':'SVCAMT','TRANTYPE':'SFTRANTYPE','CARDNUM':'SFCARDNUM'})
		for i, row in new_services.iterrows():
			if row['SFCARDNUM'] != '':
				new_services.loc[i,'SFCREDCARD'] = 'Y'
		services = services.append(new_services)

		services['D1'] = services.duplicated()
		services = services.drop_duplicates()

	for i,row in services.iterrows():
		if 'D1' in services.columns:
			if row['D1'] == True:
				deleted_records.append([row['RECKEY'],'Services:Complete Record Matched'])

	services = services.reindex(columns=['RECKEY','SVCCODE','SVCDESC','SVCAMT','MONEYTYPE','TRANDATE','VENDORCODE','TAX1','TAX2','TAX3','TAX4','SFTRANTYPE','SFCREDCARD','SFCARDNUM','MCO'])
	
	services.to_csv('{}\Tcservices.csv'.format(date),index=False)

	# Here is the code for udids.
	# Dictionaries are defined for moving break data from udids to the appropriate field in tctrips, by country.
	udids = d['TCUDIDS']

	ud33Ids = []
	ud05Ids = []
	ud32Ids = []
	ud33ToTrips = {}
	hotel_except = {}
	air_except = {}
	it_nike_break2 = {}
	nike_break3 = {}
	it_rh_break2 = {}
	merck_break2 = {}
	merck_break3 = {}
	
	print("Welcom to TCUDIDS Section")

	# print(d['TCUDIDS']['UDIDNO'].unique())
	print('---------------------------------------------------------------------------------------')
	# print(udids['UDIDNO'].unique())
	# exit()
################################################# From Here we have documentation ###############################################
	if len(udids) != 0:
		# [fs] delete any entries with no uduid number
		udids = udids.loc[udids.UDIDNO.notnull()]
		udids["UDIDNO"] = udids["UDIDNO"].astype(str)
		udids = udids[udids["UDIDNO"] != ""]
		udids.reset_index(drop=True, inplace=True)
		# udids.drop(udids[(udids.UDIDNO.isnull())].index, inplace=True)

	# udids = udids.applymap(lambda x: str(x).upper().strip().replace(',','').replace('NAN',''))
		udids = udids.applymap(lambda x: str(x).upper().strip().replace(',', '').replace('NAN','') if x == "" or x == "nan" else str(x).upper().strip().replace(',', ''))
		udids = udids.fillna('')
		udids = udids[udids.UDIDNO != '`']

		udids['UDIDNO'] = udids['UDIDNO'].astype(float)
		udids['UDIDNO'] = udids['UDIDNO'].fillna(0)
		udids['UDIDNO'] = udids['UDIDNO'].astype(int)
		udids['UDIDNO'] = udids['UDIDNO'].astype(str)
		udids['UDIDNO'] = udids['UDIDNO'].apply(lambda x: x.replace('0','') if x== '0' else x )
		# These lines strip out a whole slew of invalid values in UDIDTEXT.
		# This code strips out common errors in email addresses contained in UDIDTEXT, invalid (non-Latin) characters, and
		# letters in the UDIDNO field.

		text = udids['UDIDTEXT']
		p = re.compile(r'[^A-Za-z0-9.\s\-\/]')
		new_text = [p.sub('', x) for x in text]
		newer_text = []
		for i in new_text:
			newer_text.append(str(i).replace('NIKE','@NIKE').replace('ECOM','E.COM').replace('REDHAT','@REDHAT').replace('ATCOM','AT.COM'))
			
		udids['UDIDTEXT'] = newer_text
		numbers = udids['UDIDNO']
		new_numbers = []
		for i in numbers:
			new_numbers.append(re.sub('[^0-9\.]', '', i))
		# new_numbers = [i for i in new_numbers if i != '' else i]
		new_numbers = pd.Series(new_numbers)
		# udids['UDIDNO'] = new_numbers
		# for i in new_numbers:
		# 	print(i)
		# 	xxx = int(float(i))
		#
		# udidnos = new_numbers.map(lambda x: int(float(x)))
		udids.drop(udids[udids.UDIDNO==''].index,inplace=True)
		udids['UDIDNO'] = udids['UDIDNO'].astype(int)
		
		udids509 = udids[udids.UDIDNO == 509]
		
		# These lines strip out a whole slew of invalid values in UDIDTEXT.
		not_allowed = ['', '0', 0, '0000', '00', '000', 'NO COST CENTER', 'NO COST CENTRE', 'NONE', '1', '00000', '000000', 'N/A', '.', 'X', 'XX']
		udids = udids[~udids['UDIDTEXT'].isin(not_allowed)] # this line removes all udids that are in the not_allowed array
		
		
		# udids = udids[udids.UDIDTEXT != '']
		# udids = udids[udids.UDIDTEXT != '0']
		# udids = udids[udids.UDIDTEXT != 0]
		# udids = udids[udids.UDIDTEXT != '0000']
		# udids = udids[udids.UDIDTEXT != '00']
		# udids = udids[udids.UDIDTEXT != '000']
		# udids = udids[udids.UDIDTEXT != 'NO COST CENTER']
		# udids = udids[udids.UDIDTEXT != 'NO COST CENTRE']
		# udids = udids[udids.UDIDTEXT != 'NONE']
		# udids = udids[udids.UDIDTEXT != '1']
		# udids = udids[udids.UDIDTEXT != '00000']
		# udids = udids[udids.UDIDTEXT != '000000']
		# udids = udids[udids.UDIDTEXT != 'N/A']
		# udids = udids[udids.UDIDTEXT != '.']
		# udids = udids[udids.UDIDTEXT != 'X']
		# udids = udids[udids.UDIDTEXT != 'XX']
		# This next loop implements all of the country specific rules for each udid.
		
		# SO THE RESULT IS THAT WE FOUND OUT UDIDNO IS BY DEFAULT OF TYPE STRING AND WE NEED TO DEAL IT AS STRING IN QUOTES
		# ELSE NUMPHY WILL NOT GIVE CORRECT RESULTS
		conditions = [
			udids['UDIDNO'].eq('1') & (udids['RECKEY'].str.startswith('14505') | udids['RECKEY'].str.startswith('21305')), # updated_value = 28 in updated_values array at index 0
			udids['UDIDNO'].eq('2') & (udids['RECKEY'].str.startswith('14505') | udids['RECKEY'].str.startswith('21305')), # updated_value = 27 in updated_values array at index 1
			udids['UDIDNO'].eq('3') & (udids['RECKEY'].str.startswith('14505') | udids['RECKEY'].str.startswith('21305')), # updated_value = 32 in updated_values array at index 2
			udids['UDIDNO'].eq('4') & (udids['RECKEY'].str.startswith('14505') | udids['RECKEY'].str.startswith('21305')), # updated_value = 35 in updated_values array at index 3
			udids['UDIDNO'].eq('5') & (udids['RECKEY'].str.startswith('14505') | udids['RECKEY'].str.startswith('21305')), # updated_value = 29 in updated_values array at index 4
			udids['UDIDNO'].eq('6') & (udids['RECKEY'].str.startswith('14505') | udids['RECKEY'].str.startswith('21305')), # updated_value = 75 in updated_values array at index 5
			udids['UDIDNO'].eq('8') & udids['RECKEY'].str.startswith('213'), # updated_value = 39 in updated_values array at index 6
			udids['UDIDNO'].eq('9') & udids['RECKEY'].str.startswith('213'), # updated_value = 5 in updated_values array at index 7
			udids['UDIDNO'].eq('9') & udids['RECKEY'].str.startswith('14505'), # updated_value = 5 in updated_values array at index 8
			udids['UDIDNO'].eq('205') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 5 in updated_values array at index 9
			udids['UDIDNO'].eq('217') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 17 in updated_values array at index 10
			udids['UDIDNO'].eq('223') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 23 in updated_values array at index 11
			udids['UDIDNO'].eq('227') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 27 in updated_values array at index 12
			udids['UDIDNO'].eq('228') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 28 in updated_values array at index 13
			udids['UDIDNO'].eq('229') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 29 in updated_values array at index 14
			udids['UDIDNO'].eq('232') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 32 in updated_values array at index 15
			udids['UDIDNO'].eq('235') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 35 in updated_values array at index 16
			udids['UDIDNO'].eq('239') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 39 in updated_values array at index 17
			udids['UDIDNO'].eq('241') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 41 in updated_values array at index 18
			udids['UDIDNO'].eq('275') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 75 in updated_values array at index 19
			udids['UDIDNO'].eq('292') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 92 in updated_values array at index 20
			udids['UDIDNO'].eq('296') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 96 in updated_values array at index 21
			udids['UDIDNO'].eq('1') & (udids['RECKEY'].str.startswith('20505') | udids['RECKEY'].str.startswith('33405')), # updated_value = 17 in updated_values array at index 22
			udids['UDIDNO'].eq('39') & udids['RECKEY'].str.startswith('11505'), # updated_value = 28 in updated_values array at index 23
			udids['UDIDNO'].eq('32') & udids['RECKEY'].str.startswith('11505'), # updated_value = 3 in updated_values array at index 24
			udids['UDIDNO'].eq('8') & udids['RECKEY'].str.startswith('11505'), # updated_value = 75 in updated_values array at index 25
			udids['UDIDNO'].eq('7') & udids['RECKEY'].str.startswith('11505'), # updated_value = 35 in updated_values array at index 26
			udids['UDIDNO'].eq('6') & udids['RECKEY'].str.startswith('11505'), # updated_value = 32 in updated_values array at index 27
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('11505'), # updated_value = 10 in updated_values array at index 28
			udids['UDIDNO'].eq('5') & udids['RECKEY'].str.startswith('110'), # updated_value = 39 in updated_values array at index 29
			udids['UDIDNO'].eq('5') & udids['RECKEY'].str.startswith('115'), # updated_value = 39 in updated_values array at index 30
			udids['UDIDNO'].eq('5') & udids['RECKEY'].str.startswith('178'), # updated_value = 31 in updated_values array at index 31
			udids['UDIDNO'].eq('5') & udids['RECKEY'].str.startswith('21306'), # updated_value = 29 in updated_values array at index 32
			udids['UDIDNO'].eq('5') & udids['RECKEY'].str.startswith('21308'), # updated_value = 39 in updated_values array at index 33
			udids['UDIDNO'].eq('5') & udids['RECKEY'].str.startswith('21303'), # updated_value = 39 in updated_values array at index 34
			udids['UDIDNO'].eq('2') & udids['RECKEY'].str.startswith('273'), # updated_value = 39 in updated_values array at index 35
			udids['UDIDNO'].eq('2') & udids['RECKEY'].str.startswith('150'), # updated_value = 39 in updated_values array at index 36
			udids['UDIDNO'].eq('1') & udids['RECKEY'].str.startswith('178'), # updated_value = 39 in updated_values array at index 37
			udids['UDIDNO'].eq('2') & udids['RECKEY'].str.startswith('178'), # updated_value = 39 in updated_values array at index 38
			udids['UDIDNO'].eq('3') & udids['RECKEY'].str.startswith('178'), # updated_value = 39 in updated_values array at index 39
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('178'), # updated_value = 39 in updated_values array at index 40
			udids['UDIDNO'].eq('2') & udids['RECKEY'].str.startswith('21306'), # updated_value = 39 in updated_values array at index 41
			udids['UDIDNO'].eq('6') & udids['RECKEY'].str.startswith('21306'), # updated_value = 39 in updated_values array at index 42
			udids['UDIDNO'].eq('10') & udids['RECKEY'].str.startswith('21306'), # updated_value = 39 in updated_values array at index 43
			udids['UDIDNO'].eq('1') & udids['RECKEY'].str.startswith('21306'), # updated_value = 39 in updated_values array at index 44
			udids['UDIDNO'].eq('7') & udids['RECKEY'].str.startswith('21308'), # updated_value = 39 in updated_values array at index 45
			udids['UDIDNO'].eq('10') & udids['RECKEY'].str.startswith('21308'), # updated_value = 39 in updated_values array at index 46
			udids['UDIDNO'].eq('1') & udids['RECKEY'].str.startswith('21308'), # updated_value = 39 in updated_values array at index 47
			udids['UDIDNO'].eq('3') & udids['RECKEY'].str.startswith('21303'), # updated_value = 39 in updated_values array at index 48
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('21303'), # updated_value = 39 in updated_values array at index 49
			udids['UDIDNO'].eq('1') & udids['RECKEY'].str.startswith('293'), # updated_value = 39 in updated_values array at index 50
			udids['UDIDNO'].eq('3') & udids['RECKEY'].str.startswith('293'), # updated_value = 39 in updated_values array at index 51
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('11004'), # updated_value = 39 in updated_values array at index 52
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('11504'), # updated_value = 39 in updated_values array at index 53
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('21306'), # updated_value = 39 in updated_values array at index 54
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('11002'), # updated_value = 39 in updated_values array at index 55
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('21308'), # updated_value = 39 in updated_values array at index 56
			udids['UDIDNO'].eq('4') & udids['RECKEY'].str.startswith('11506'), # updated_value = 39 in updated_values array at index 57
		]

		updated_values = [
			'28', #index 0
			'27', #index 1
			'32', #index 2
			'35', #index 3
			'29', #index 4
			'75', #index 5
			'39', #index 6
			'5', #index 7
			'5', #index 8
			'5', #index 9
			'17', #index 10
			'23', #index 11
			'27', #index 12
			'28', #index 13
			'29', #index 14
			'32', #index 15
			'35', #index 16
			'39', #index 17
			'41', #index 18
			'75', #index 19
			'92', #index 20
			'96', #index 21
			'17', #index 22
			'28', #index 23
			'3', #index 24
			'75', #index 25
			'35', #index 26
			'32', #index 27
			'10', #index 28
			'39', #index 29
			'39', #index 30
			'31', #index 31
			'29', #index 32
			'39', #index 33
			'39', #index 34
			'5', #index 35
			'29', #index 36
			'29', #index 37
			'28', #index 38
			'39', #index 39
			'17', #index 40
			'27', #index 41
			'17', #index 42
			'39', #index 43
			'28', #index 44
			'17', #index 45
			'23', #index 46
			'28', #index 47
			'17', #index 48
			'23', #index 49
			'17', #index 50
			'29', #index 51
			'39', #index 52
			'39', #index 53
			'39', #index 54
			'39', #index 55
			'39', #index 56
			'39', #index 57
		]

		udids['UDIDNO'] = np.select(conditions, updated_values, default=udids['UDIDNO'])
		print('=============== multiple conditions executed in one line =================')		
		udids.drop(udids[(udids['UDIDNO'] == '27') & (udids['RECKEY'].str.startswith('150') == True)].index, inplace=True)
		# drop all udids with UDIDNO = 28 and UDIDTEXT is not digit and RECKEY starts with 150
		udids.drop(udids[(udids['UDIDNO'] == '28') & (udids['RECKEY'].str.startswith('150') == True) & (udids['UDIDTEXT'].str.isdigit() == True)].index, inplace=True)
		# drop all udids with UDIDNO = 5 and UDIDTEXT is not in ['ONLINE', 'ON LINE', 'EZGO', 'W']
		udids.drop(udids[(udids['UDIDNO'] == '5') & (udids['UDIDTEXT'].isin(['ONLINE', 'ON LINE', 'EZGO', 'W']) == False)].index, inplace=True)
		# print('len of udids before ', len(udids))
		UD12_reck_keys_to_remove = udids.loc[ (udids['RECKEY'].isin(UD12Trips)) & ( (udids['UDIDNO'].eq('12')) | (udids['UDIDNO'].eq(12)) ) ][['RECKEY']].values		
		
		
		# TODO: below if condition has been taken out of loop for i, row in udids.iterrows(): the above 2 lines solution is used instead of loop
		# TODO: but we need to verify that it works correctly
		# Italy & Switzerland Accounts , IT :13 , CH : 35 , Merck : 05
		""" if row['RECKEY'] in UD12Trips and row['UDIDNO'] == '12':
			
			UD12Trips.remove(str(row['RECKEY'])) """

		UD11_reck_keys_to_remove = udids.loc[ (udids['RECKEY'].isin(UD11Trips)) & ( (udids['UDIDNO'].eq('11')) | (udids['UDIDNO'].eq(11)) ) ][['RECKEY']].values
		UD11Trips = [x for x in UD11Trips if x not in UD11_reck_keys_to_remove]
		""" if row['RECKEY'] in UD11Trips and row['UDIDNO'] == '11':
				
				UD11Trips.remove(str(row['RECKEY'])) """

		UD32_RECKEYS_to_append =  pd.Series(udids.loc[ (udids['RECKEY'].isin(UD32Trips)) & ( (udids['UDIDNO'].eq('32')) | (udids['UDIDNO'].eq(32)) ) ]['RECKEY'].values).to_list()
		ud32Ids.extend(UD32_RECKEYS_to_append)
		print(ud32Ids, ' is ud32Ids')
		""" if row['RECKEY'] in UD32Trips and row['UDIDNO'] == '32':
				
				ud32Ids.append(row['RECKEY']) """
		UD33_RECKEYS_to_append = pd.Series(udids.loc[ ( (udids['UDIDNO'].eq('33')) | (udids['UDIDNO'].eq(33)) ) ]['RECKEY'].values).to_list()
		ud33Ids.extend(UD33_RECKEYS_to_append)
		"""
			if row['UDIDNO'] == '33':
				ud33Ids.append(row['RECKEY'])
		"""
		# Merck UK accounts UDIDNO remap... UK:17 Merck :05
		conditions = [ udids['RECKEY'].str.startswith('33405') & udids['UDIDTEXT'].eq('GB') ]
		updated_values = [ 'UK' ]
		udids['UDIDTEXT'] = np.select(conditions, updated_values, default=udids['UDIDTEXT'])
		""" if str(row['RECKEY'])[0:5] == '33405':
			if row['UDIDTEXT'] == 'GB':
				udids.loc[i,'UDIDTEXT'] = 'UK' """

		TempDataFrame = udids.loc[ 
			( 
				(udids['UDIDNO'].eq('5')) & (udids['RECKEY'].str.startswith('21306')) | 
				(
					(udids['UDIDNO'].eq('29')) & 
					(
						udids['RECKEY'].str.startswith('16106') |
						udids['RECKEY'].str.startswith('17306') |
						udids['RECKEY'].str.startswith('26906') |
						udids['RECKEY'].str.startswith('29906')									
					)
				
				)
			)
		]
		temp_dict = pd.Series(TempDataFrame['RECKEY'].values,index=TempDataFrame['UDIDTEXT'].values).to_dict()
		nike_break3.update(temp_dict) # nike_break3 is a dictionary with key as RECKEY and value as UDIDTEXT, we have appended/updated it in this line with the new values
		""" if row['UDIDNO'] == '5' and str(row['RECKEY'])[0:5] == '21306':
			nike_break3[str(row['RECKEY'])]  = row['UDIDTEXT']
		if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[0:5] == '16106':
			nike_break3[str(row['RECKEY'])] = row['UDIDTEXT']
		if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[0:5] == '17306':
			nike_break3[str(row['RECKEY'])] = row['UDIDTEXT']
		if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[0:5] == '26906':
			nike_break3[str(row['RECKEY'])] = row['UDIDTEXT']
		if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[0:5] == '29906':
			nike_break3[str(row['RECKEY'])] = row['UDIDTEXT'] """

		TempDataFrame = udids.loc[ (udids['UDIDNO'].eq('15')) & (udids['RECKEY'].str.startswith('21306')) ]
		temp_dict = pd.Series(TempDataFrame['RECKEY'].values,index=TempDataFrame['UDIDTEXT'].values).to_dict()
		hotel_except.update(temp_dict) # this line appends the new values in temp_dict that meth the conditions, to the dictionary hotel_except
		""" if row['UDIDNO'] == '15' and str(row['RECKEY'])[0:5] == '21306':
			hotel_except[row['RECKEY']]  = row['UDIDTEXT'] """

		TempDataFrame = udids.loc[ (udids['UDIDNO'].eq('1')) & (udids['RECKEY'].str.startswith('21306')) ]
		temp_dict = pd.Series(TempDataFrame['RECKEY'].values,index=TempDataFrame['UDIDTEXT'].values).to_dict()
		it_nike_break2.update(temp_dict) # this line appends the new values in temp_dict that meth the conditions, to the dictionary it_nike_break2
		""" if row['UDIDNO'] == '1' and str(row['RECKEY'])[0:5] == '21306':
			it_nike_break2[str(row['RECKEY'])]  = row['UDIDTEXT'] """

		TempDataFrame = udids.loc[ (udids['UDIDNO'].eq('13')) & (udids['RECKEY'].str.startswith('21308')) ]
		temp_dict = pd.Series(TempDataFrame['RECKEY'].values,index=TempDataFrame['UDIDTEXT'].values).to_dict()
		air_except.update(temp_dict) # this line appends the new values in temp_dict that meth the conditions, to the dictionary air_except
		""" if row['UDIDNO'] == '13' and str(row['RECKEY'])[0:5] == '21308':
			air_except[row['RECKEY']]  = row['UDIDTEXT'] """

		TempDataFrame = udids.loc[ (udids['UDIDNO'].eq('1')) & (udids['RECKEY'].str.startswith('21308')) ]
		temp_dict = pd.Series(TempDataFrame['RECKEY'].values,index=TempDataFrame['UDIDTEXT'].values).to_dict()
		it_rh_break2.update(temp_dict) # this line appends the new values in temp_dict that meth the conditions, to the dictionary it_rh_break2
		""" if row['UDIDNO'] == '1' and str(row['RECKEY'])[0:5] == '21308':
			it_rh_break2[str(row['RECKEY'])]  = row['UDIDTEXT'] """

		# update all udids set UDIDTEXT to 'HOTEL CAR ONLY' where UDIDNO = 11
		udids.loc[ (udids['UDIDNO'].eq('11')) | (udids['UDIDNO'].eq(11)) , 'UDIDTEXT'] = 'HOTEL CAR ONLY'
		""" if row['UDIDNO'] == '11':
				udids.loc[i,'UDIDTEXT']= 'HOTEL CAR ONLY' """
		print('============= before the udids loop at 1781 =============')
		# print_elapsed_time()
		for i, row in udids.iterrows():
			
			""" if row['UDIDNO'] == '33':
				if row['UDIDTEXT'] != 0:
					ud33ToTrips[row['RECKEY']] = row['UDIDTEXT'] # TODO: ud33ToTrips is not used, so we haven't written its alternative code outside loop """
			
			# Merck UK accounts UDIDNO remap... UK:17 Merck :05
			if str(row['RECKEY'])[0:5] == '33405':
				if 	str(row['UDIDNO'])[0:1] == '2':
					udids.loc[i,'UDIDNO'] = str(row['UDIDNO']).replace(str(row['UDIDNO'])[0:1],'')
				if 	str(row['UDIDNO'])[0:2] == '20':
					udids.loc[i,'UDIDNO'] = str(row['UDIDNO']).replace(str(row['UDIDNO'])[0:2],'')
			
			if str(row['UDIDNO']) == '35' and str(row['RECKEY'])[3:5] == '05':
				merck_break2[str(row['RECKEY'])] = row['UDIDTEXT']
			if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[3:5] == '05':
				merck_break3[str(row['RECKEY'])] = row['UDIDTEXT']		

		# for i, row in udids.iterrows():

		# 	# Italy & Switzerland Accounts , IT :13 , CH : 35 , Merck : 05
		# 	if row['RECKEY'] in UD12Trips and row['UDIDNO'] == 12:
		# 		UD12Trips.remove(str(row['RECKEY'])) 
			
		# 	if row['RECKEY'] in UD11Trips and row['UDIDNO'] == 11:
		# 		UD11Trips.remove(str(row['RECKEY']))
			
		# 	if row['RECKEY'] in UD32Trips and row['UDIDNO'] == 32:
		# 		ud32Ids.append(row['RECKEY'])
			
		# 	if row['UDIDNO'] == 33:
		# 		ud33Ids.append(row['RECKEY'])
		# 		if row['UDIDTEXT'] != 0:
		# 			# ud33ToTrips.append([row['RECKEY'],row['UDIDTEXT']])
		# 			ud33ToTrips[row['RECKEY']] = row['UDIDTEXT']

		# 	if row['UDIDNO'] == 1 and (str(row['RECKEY'])[0:5] == '14505' or str(row['RECKEY'])[0:5] == '21305'):
		# 		udids.loc[i,'UDIDNO'] = 28 
		# 	if row['UDIDNO'] == 2 and (str(row['RECKEY'])[0:5] == '14505' or str(row['RECKEY'])[0:5] == '21305'):
		# 		udids.loc[i,'UDIDNO'] = 27
		# 	if row['UDIDNO'] == 3 and (str(row['RECKEY'])[0:5] == '14505' or str(row['RECKEY'])[0:5] == '21305'):
		# 		udids.loc[i,'UDIDNO'] = 32
		# 	if row['UDIDNO'] == 4 and (str(row['RECKEY'])[0:5] == '14505' or str(row['RECKEY'])[0:5] == '21305'):
		# 		udids.loc[i,'UDIDNO'] = 35
		# 	if row['UDIDNO'] == 5 and (str(row['RECKEY'])[0:5] == '14505' or str(row['RECKEY'])[0:5] == '21305'):
		# 		udids.loc[i,'UDIDNO'] = 29
		# 	if row['UDIDNO'] == 6 and (str(row['RECKEY'])[0:5] == '14505' or str(row['RECKEY'])[0:5] == '21305'):
		# 		udids.loc[i,'UDIDNO'] = 75  
		# 	# Merck UK accounts UDIDNO remap... UK:17 Merck :05
		# 	if str(row['RECKEY'])[0:5] == '33405':
		# 		if row['UDIDTEXT'] == 'GB':
		# 			udids.loc[i,'UDIDTEXT'] = 'UK' 
		# 		if 	str(row['UDIDNO'])[0:1] == '2':
		# 			udids.loc[i,'UDIDNO'] = str(row['UDIDNO']).replace(str(row['UDIDNO'])[0:1],'')
		# 		if 	str(row['UDIDNO'])[0:2] == '20':
		# 			udids.loc[i,'UDIDNO'] = str(row['UDIDNO']).replace(str(row['UDIDNO'])[0:2],'')

		# 	if row['UDIDNO'] == 8  and str(row['RECKEY'])[0:3] == '213':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 9  and str(row['RECKEY'])[0:3] == '14505':
		# 		udids.loc[i,'UDIDNO'] = 5
		# 	if row['UDIDNO'] == 9  and str(row['RECKEY'])[0:3] == '213':
		# 		udids.loc[i,'UDIDNO'] = 5
		# 	if row['UDIDNO'] == 205 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 5
		# 	if row['UDIDNO'] == 217 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 17
		# 	if row['UDIDNO'] == 223 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 23
		# 	if row['UDIDNO'] == 227 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 27
		# 	if row['UDIDNO'] == 228 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 28
		# 	if row['UDIDNO'] == 229 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 29
		# 	if row['UDIDNO'] == 232 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 32
		# 	if row['UDIDNO'] == 235 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 35
		# 	if row['UDIDNO'] == 239 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 241 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 41
		# 	if row['UDIDNO'] == 275 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 75
		# 	if row['UDIDNO'] == 292 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 92
		# 	if row['UDIDNO'] == 296 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 96
		# 	if row['UDIDNO'] == 1 and (str(row['RECKEY'])[0:5] == '20505' or str(row['RECKEY'])[0:5] == '33405'):
		# 		udids.loc[i,'UDIDNO'] = 17
		# 	if row['UDIDNO'] == 39 and (str(row['RECKEY'])[0:5]) == '11505':
		# 		udids.loc[i,'UDIDNO'] = 28
		# 	if row['UDIDNO'] == 32 and (str(row['RECKEY'])[0:5]) == '11505':
		# 		udids.loc[i,'UDIDNO'] = 3
		# 	if row['UDIDNO'] == 8 and (str(row['RECKEY'])[0:5]) == '11505':
		# 		udids.loc[i,'UDIDNO'] = 75
		# 	if row['UDIDNO'] == 7 and (str(row['RECKEY'])[0:5]) == '11505':
		# 		udids.loc[i,'UDIDNO'] = 35
		# 	if row['UDIDNO'] == 6 and (str(row['RECKEY'])[0:5]) == '11505':
		# 		ud32Ids.append(row['RECKEY'])
		# 		udids.loc[i,'UDIDNO'] = 32
		# 	if row['UDIDNO'] == 4 and (str(row['RECKEY'])[0:5]) == '11505':
		# 		udids.loc[i,'UDIDNO'] = 10
		# 	if row['UDIDNO'] == 5 and str(row['RECKEY'])[0:3] == '110':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 5 and str(row['RECKEY'])[0:3] == '115':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 5 and str(row['RECKEY'])[0:3] == '178':
		# 		udids.loc[i,'UDIDNO'] = 31
		# 	if row['UDIDNO'] == 5 and str(row['RECKEY'])[0:5] == '21306':
		# 		udids.loc[i,'UDIDNO'] = 29
		# 		nike_break3[str(row['RECKEY'])]  = row['UDIDTEXT']
		# 	if row['UDIDNO'] == 5 and str(row['RECKEY'])[0:5] == '21308':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 5 and str(row['RECKEY'])[0:5] == '21303':
		# 		udids.loc[i,'UDIDNO'] = 39

		# for i, row in udids.iterrows():
		# 	if row['UDIDNO'] == 2 and str(row['RECKEY'])[0:3] == '273':
		# 		udids.loc[i,'UDIDNO'] = 5
		# 	if row['UDIDNO'] == 2 and str(row['RECKEY'])[0:3] == '150':
		# 		udids.loc[i,'UDIDNO'] = 29
		# 	if row['UDIDNO'] == 27 and str(row['RECKEY'])[0:3] == '150':
		# 		udids.drop(i,inplace=True)
		# 	if row['UDIDNO'] == 28 and row['UDIDTEXT'].isdigit() == False and str(row['RECKEY'])[0:3] == '150':
		# 		udids.drop(i,inplace=True)
		# 	if row['UDIDNO'] == 5 and row['UDIDTEXT'] != 'ONLINE' and row['UDIDNO'] == 5 and row['UDIDTEXT'] != 'ON LINE' and row['UDIDNO'] == 5 and row['UDIDTEXT'] != 'EZGO' and row['UDIDNO'] == 5 and row['UDIDTEXT'] != 'W':
		# 		udids.drop(i,inplace=True)
		# 	if row['UDIDNO'] == 1 and str(row['RECKEY'])[0:3] == '178':
		# 		udids.loc[i,'UDIDNO'] = 29
		# 	if row['UDIDNO'] == 2 and str(row['RECKEY'])[0:3] == '178':
		# 		udids.loc[i,'UDIDNO'] = 28
		# 	if row['UDIDNO'] == 3 and str(row['RECKEY'])[0:3] == '178':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 4 and str(row['RECKEY'])[0:3] == '178':
		# 		udids.loc[i,'UDIDNO'] = 17
		# 	if row['UDIDNO'] == 15 and str(row['RECKEY'])[0:5] == '21306':
		# 		hotel_except[row['RECKEY']]  = row['UDIDTEXT']
		# 	if row['UDIDNO'] == 2 and str(row['RECKEY'])[0:5] == '21306':
		# 		udids.loc[i,'UDIDNO'] = 27
		# 	if row['UDIDNO'] == 6 and str(row['RECKEY'])[0:5] == '21306':
		# 		udids.loc[i,'UDIDNO'] = 17
		# 	if row['UDIDNO'] == 10 and str(row['RECKEY'])[0:5] == '21306':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 1 and str(row['RECKEY'])[0:5] == '21306':
		# 		udids.loc[i,'UDIDNO'] = 28
		# 		it_nike_break2[str(row['RECKEY'])]  = row['UDIDTEXT']
		# 	if row['UDIDNO'] == 13 and str(row['RECKEY'])[0:5] == '21308':
		# 		air_except[row['RECKEY']]  = row['UDIDTEXT']
		# 	if row['UDIDNO'] == 7 and str(row['RECKEY'])[0:5] == '21308':
		# 		udids.loc[i,'UDIDNO'] = 17
		# 	if row['UDIDNO'] == 10 and str(row['RECKEY'])[0:5] == '21308':
		# 		udids.loc[i,'UDIDNO'] = 23
		# 	if row['UDIDNO'] == 1 and str(row['RECKEY'])[0:5] == '21308':
		# 		udids.loc[i,'UDIDNO'] = 28
		# 		it_rh_break2[str(row['RECKEY'])]  = row['UDIDTEXT']
		# 	if row['UDIDNO'] == 3 and str(row['RECKEY'])[0:5] == '21303':
		# 		udids.loc[i,'UDIDNO'] = 17
		# 	if row['UDIDNO'] == 4 and str(row['RECKEY'])[0:5] == '21303':
		# 		udids.loc[i,'UDIDNO'] = 23
		# 	if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[0:5] == '16106':
		# 		nike_break3[str(row['RECKEY'])] = row['UDIDTEXT']
		# 	if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[0:5] == '17306':
		# 		nike_break3[str(row['RECKEY'])] = row['UDIDTEXT']
		# 	if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[0:5] == '26906':
		# 		nike_break3[str(row['RECKEY'])] = row['UDIDTEXT']
		# 	if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[0:5] == '29906':
		# 		nike_break3[str(row['RECKEY'])] = row['UDIDTEXT']
		# 	if row['UDIDNO'] == 1 and str(row['RECKEY'])[0:3] == '293':
		# 		udids.loc[i,'UDIDNO'] = 17
		# 	if row['UDIDNO'] == 3 and str(row['RECKEY'])[0:3] == '293':
		# 		udids.loc[i,'UDIDNO'] = 29
		# 	if row['UDIDNO'] == 11:
		# 		udids.loc[i,'UDIDTEXT']= 'HOTEL CAR ONLY'
		# 	if row['UDIDNO'] == 4 and str(row['RECKEY'])[0:5] == '11004':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 4 and str(row['RECKEY'])[0:5] == '11504':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 4 and str(row['RECKEY'])[0:5] == '21306':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 4 and str(row['RECKEY'])[0:5] == '11002':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 4 and str(row['RECKEY'])[0:5] == '21308':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if row['UDIDNO'] == 4 and str(row['RECKEY'])[0:5] == '11506':
		# 		udids.loc[i,'UDIDNO'] = 39
		# 	if str(row['UDIDNO']) == '35' and str(row['RECKEY'])[3:5] == '05':
		# 		merck_break2[str(row['RECKEY'])] = row['UDIDTEXT']
		# 	if str(row['UDIDNO']) == '29' and str(row['RECKEY'])[3:5] == '05':
		# 		merck_break3[str(row['RECKEY'])] = row['UDIDTEXT']

	# These lines take the values from TCTRIPS and turn them into the appropriate udids, such as ud5 for online booking.
	temp_arrays = []
	for i in online_booking:
		temp_arrays.append([i,'5','ONLINE'])
		# df = pd.DataFrame([[i,'5','ONLINE']],columns=['RECKEY','UDIDNO','UDIDTEXT'])
		# udids = udids.append(df)
	for i in de_UD39:
		temp_arrays.append([i[0],'39',i[-1]])
		# df = pd.DataFrame([[i[0],'39',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
		# udids = udids.append(df)
	for i in UD27:
		temp_arrays.append([i[0],'27',i[-1]])
		# df = pd.DataFrame([[i[0],'27',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
		# udids = udids.append(df)
	for i in UD409:
		if i[0] not in UD409_trips_reckeys:
			temp_arrays.append([i[0],'409',i[-1]])
			# df = pd.DataFrame([[i[0],'409',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
			# udids = udids.append(df)
	for i in ud54:
		temp_arrays.append([i[0],'54',i[1]])
		# df = pd.DataFrame([[i[0],'54',i[1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
		# udids = udids.append(df)
	##print(UD12Trips)	
	for i in UD12Trips:
		temp_arrays.append([i,'12','NH'])
		# df = pd.DataFrame([[i,'12','NH']],columns=['RECKEY','UDIDNO','UDIDTEXT'])
		# udids = udids.append(df)
	for i in UD11Trips:
		temp_arrays.append([i,'11','HOTEL CAR ONLY'])
		# df = pd.DataFrame([[i,'11','HOTEL CAR ONLY']],columns=['RECKEY','UDIDNO','UDIDTEXT'])
		# udids = udids.append(df)
	for i, row in UD32Trips:
		if i not in ud32Ids:
			temp_arrays.append([i,'32',row])
			# df = pd.DataFrame([[i,'32',row]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
			# udids = udids.append(df,ignore_index=True)	
	for i, row in UD33Trips:
		if i not in ud33Ids:
			temp_arrays.append([i,'33',row])
			# df = pd.DataFrame([[i,'33',row]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
			# udids = udids.append(df,ignore_index=True)

	if len(temp_arrays) > 0:
			temp_dfs = pd.DataFrame(temp_arrays,columns=['RECKEY','UDIDNO','UDIDTEXT'])
			udids = udids.append(temp_dfs,ignore_index=True)
	print('udids length after => ', len(udids))
	
	# for i in online_booking:
	# 	df = pd.DataFrame([[i,'5','ONLINE']],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 	udids = udids.append(df)
	# for i in de_UD39:
	# 	df = pd.DataFrame([[i[0],'39',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 	udids = udids.append(df)
	# for i in UD27:
	# 	df = pd.DataFrame([[i[0],'27',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 	udids = udids.append(df)
	# for i in UD409:
	# 	if i[0] not in UD409_trips_reckeys:
	# 		df = pd.DataFrame([[i[0],'409',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 		udids = udids.append(df)
	# for i in ud54:
	# 	df = pd.DataFrame([[i[0],'54',i[1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 	udids = udids.append(df)	
	# for i in UD12Trips:
	# 	df = pd.DataFrame([[i,'12','NH']],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 	udids = udids.append(df)
	# for i in UD11Trips:
	# 	df = pd.DataFrame([[i,'11','HOTEL CAR ONLY']],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 	udids = udids.append(df)
	# for i, row in UD32Trips:
	# 	if i not in ud32Ids:
	# 		df = pd.DataFrame([[i,'32',row]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 		udids = udids.append(df,ignore_index=True)	
	# for i, row in UD33Trips:
	# 	if i not in ud33Ids:
	# 		df = pd.DataFrame([[i,'33',row]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
	# 		udids = udids.append(df,ignore_index=True)

	# udids = udids.fillna('')
	
	for i,row in udids.iterrows():
		if row['UDIDNO'] == 5 and (row['UDIDTEXT'] =='ONLINE' or row['UDIDTEXT'] =='EZGO'):
			ud05Ids.append(row['RECKEY'])
	if len(udids) > 0:
		udids = udids[udids.UDIDTEXT != '']

		udids['D1'] = udids.duplicated()
		udids = udids.drop_duplicates()
		
		udids['D2'] = udids.duplicated(['RECKEY','UDIDTEXT'],keep='last')
		udids.drop_duplicates(subset=['RECKEY','UDIDTEXT'],keep='first',inplace=True)

	# In this section we populate UDs using trips data like travelerId or UDIDS data UD27s to populate TRIPS columns
	tid = udids[udids.UDIDNO == 27]
	tid_dict = pd.Series(tid['UDIDTEXT'].values,index=tid.RECKEY).to_dict()
	
	div = udids[udids.UDIDNO == 29]
	div_dict = pd.Series(div['UDIDTEXT'].values,index=div.RECKEY).to_dict()

	bunit = udids[udids.UDIDNO == 35]
	bunit_dict = pd.Series(bunit['UDIDTEXT'].values,index=bunit.RECKEY).to_dict()

	atgGermanCountries=['13405','15905','11405']
	for i,row in trips.iterrows():
		if row['TRAVELERID'] == '' or str(row['TRAVELERID']) == 'nan':
			if row['RECKEY'] in tid_dict:
				trips.loc[i,'TRAVELERID'] = tid_dict[row['RECKEY']]
		if row['BREAK3'] == '' or str(row['BREAK3']) == 'nan':
			if row['RECKEY'] in div_dict:
				trips.loc[i,'BREAK3'] = div_dict[row['RECKEY']]
		if row['BREAK2'] == '' or str(row['BREAK2']) == 'nan':
			if row['RECKEY'] in bunit_dict:
				trips.loc[i,'BREAK2'] = bunit_dict[row['RECKEY']]
		
		if str(row['RECKEY'][:5]) in atgGermanCountries:	
			if row['VALCARRMOD']=='R':
				if row['RECKEY'] in ud05Ids or row['BKAGENT']=='ONLINE':
					if row['VALCARR']=='':
						row['VALCARR']='DB'
				elif row['RECKEY'] not in ud05Ids:
					if row['VALCARR']=='':
						row['VALCARR']='1R'

	trips_tid_dict = pd.Series(trips['TRAVELERID'].values,index=trips.RECKEY).to_dict()
	trips_b1_dict = pd.Series(trips["BREAK1"].values,index=trips.RECKEY).to_dict()
	trips_b2_dict = pd.Series(trips['BREAK2'].values,index=trips.RECKEY).to_dict()
	trips_b3_dict = pd.Series(trips['BREAK3'].values,index=trips.RECKEY).to_dict()

	trips_reckeys = pd.unique(pd.Series(trips['RECKEY'].values).dropna())

	# udids = udids.sort_values(by='UDIDNO', ascending=False)
	for i,row in udids.iterrows():
		if row['UDIDNO'] == 27:
			if str(row['UDIDTEXT']) == 'nan' or row['UDIDTEXT'] == '':
				if row['RECKEY'] in trips_tid_dict:
					udids.loc[i,'UDIDTEXT'] = trips_tid_dict[row['RECKEY']]
		if row['UDIDNO'] == 29:
			if str(row['UDIDTEXT']) == 'nan' or row['UDIDTEXT'] == '':
				if row['RECKEY'] in trips_b3_dict:
					udids.loc[i,'UDIDTEXT'] = trips_b3_dict[row['RECKEY']]
		if row['UDIDNO'] == 35:
			if str(row['UDIDTEXT']) == 'nan' or row['UDIDTEXT'] == '':
				if row['RECKEY'] in trips_b2_dict:
					udids.loc[i,'UDIDTEXT'] = trips_b2_dict[row['RECKEY']]

		if row['RECKEY'][:5] == "17803":
			if str(row['UDIDNO']) == "29":
				udids.loc[i,'UDIDTEXT'] = trips_b1_dict[str(row['RECKEY'])]

			if str(row['UDIDNO']) == "28":
				udids.loc[i,'UDIDTEXT'] = trips_b2_dict[str(row['RECKEY'])]

			if str(row['UDIDNO']) == "39":
				udids.loc[i,'UDIDTEXT'] = trips_b3_dict[str(row['RECKEY'])]

		# push deleted udids in deleted_records array
		if row['D1'] == True:
			deleted_records.append([row['RECKEY'],'Udids:Complete Record Matched'])
		if row['D2'] == True:
			deleted_records.append([row['RECKEY'],'Udids:RECKEY & UDIDTEXT Matched'])

		if 'D1' in udids.columns:
			del udids['D1']
		if 'D2' in udids.columns:
			del udids['D2']
		#Rule to change udidno which is increased by 200 for uk and IE
		if str(row['RECKEY'])[:3] == '335' or str(row['RECKEY'])[:3] == '205':
			udids['UDIDNO'] = udids['UDIDNO'].apply(lambda x: 31 if str(x) in ['231','231.0'] else x)
			udids['UDIDNO'] = udids['UDIDNO'].apply(lambda x: 33 if str(x) in ['233','233.0'] else x)
			udids['UDIDNO'] = udids['UDIDNO'].apply(lambda x: 4 if str(x) in ['204','204.0'] else x)
			udids['UDIDNO'] = udids['UDIDNO'].apply(lambda x: 11 if str(x) in ['211','211.0'] else x)
		
		#Rule to change udidno which is greater than 2 in size ie: i>=2 by 200 for uk
		if str(row['RECKEY'])[:3] == '335' or str(row['RECKEY'])[:3] == '337':
			# print 
			udids['UDIDNO'] = udids['UDIDNO'].apply(lambda x: str(x)[1:] if len(str(x)) >2 and str(x)[0]=='2'  else x)

	# udids = udids.drop_duplicates(subset=['RECKEY','UDIDNO'],keep= 'last')
	# udids = df.drop_duplicates(subset=['RECKEY','UDIDNO'],keep= 'last')
	# print udids
	# exit()
	# for i,row in udids.iterrows():
		# print udids[i]["RECKEY"], row[i]["UDIDNO"]
		# if row["RECKEY"] and row["UDIDNO"]
		# print i, row["RECKEY"], row["UDIDNO"], row["UDIDTEXT"]
	# exit("Hello udids")
	# df.drop_duplicates(['A','B'],keep= 'last')
	for i in trips_reckeys:
		temp = udids[udids.RECKEY == i]
		if 29 not in temp['UDIDNO'].unique():
			if i in trips_b1_dict:
				if str(trips_b1_dict[i]) != 'nan' and str(trips_b1_dict[i]) != '':
					load = pd.DataFrame([[i,29,trips_b1_dict[i]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
		if 28 not in temp['UDIDNO'].unique():
			if i in trips_b2_dict:
				if str(trips_b2_dict[i]) != 'nan' and str(trips_b2_dict[i]) != '':
					load = pd.DataFrame([[i,28,trips_b2_dict[i]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
		if 39 not in temp['UDIDNO'].unique():
			if i in trips_b1_dict:
				if str(trips_b3_dict[i]) != 'nan' and str(trips_b3_dict[i]) != '':
					load = pd.DataFrame([[i,39,trips_b3_dict[i]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)

	print("You have crossed 2000 lines :)")

	# The last code block adds the data from udids to trips, and from trips back to legs, and finally writes those two back to csv.
	
	tickets = ''
	if len(trips) >0:
		tickets = trips['ORIGTICKET']
	new_tickets = []
	for i in tickets:
		new_tickets.append(re.sub('[^0-9]', '', i))
	new_tickets = pd.Series(new_tickets)
	trips['ORIGTICKET'] = new_tickets
	# if len(trips) >0:
	# 	tickets = trips['TICKET']
	# new_tickets = []
	# for i in tickets:
	# 	new_tickets.append(re.sub('[^0-9]', '', i))
	# new_tickets = pd.Series(new_tickets)
	# trips['TICKET'] = new_tickets
	
	trips.loc[ trips['VALCARR'] == 'ZZ', ['VALCARRMOD', 'STNDCHG', 'MKTFARE', 'REASCODE', 'OFFRDCHG', 'BASEFARE', 'FARETAX', 'SVCFEE', 'TAX1', 'TAX2', 'TAX3', 'TAX4'] ] = ''
	trips.loc[ trips['VALCARR'] == 'ZZ', 'AIRCHG' ] = 0 # we need to set AIRCHG to 0 for ZZ trips
	TempDataFrame = trips.loc[ trips['VALCARR'] == 'ZZ']
	temp_dict = pd.Series('VALCARR ZZ - NOT A VALID TRIP',index=TempDataFrame['RECKEY'].values).to_dict()
	deleted_entries.update(temp_dict) # nike_break3 is a dictionary with key as RECKEY and value as UDIDTEXT, we
	
	for i, row in trips.iterrows():
		if str(row['BREAK3']).isdigit() == True and len(row['BREAK3']) == 3 and len(row['BREAK2']) != 6:
			trips.loc[i, 'BREAK2'] = row['BREAK3']
			trips.loc[i, 'BREAK3'] = ''
		if str(row['RECKEY']) in valcarr_mode:
			trips.loc[i, 'VALCARRMOD'] = valcarr_mode[str(row['RECKEY'])]
		if str(row['RECKEY']) in airlines:
			new_airlines[str(row['RECKEY'])] = row['VALCARR']
		if str(row['RECKEY']) in it_nike_break2:
			break2_info = it_nike_break2[str(row['RECKEY'])]
			trips.loc[i, 'BREAK2'] = break2_info
		if str(row['RECKEY']) in nike_break3:
			break3_info = nike_break3[str(row['RECKEY'])]
			trips.loc[i, 'BREAK3'] = break3_info
		if str(row['RECKEY']) in it_rh_break2:
			break_info = it_rh_break2[str(row['RECKEY'])]
			trips.loc[i, 'BREAK2'] = break_info
		if str(row['RECKEY']) in merck_break2:
			break_info = merck_break2[str(row['RECKEY'])]
			trips.loc[i, 'BREAK2'] = break_info
		if str(row['RECKEY']) in merck_break3:
			break_info = merck_break3[str(row['RECKEY'])]
			trips.loc[i, 'BREAK3'] = break_info
		# The following lines of code set BREAK3 to empty for SKECHERS account numbers.
		if str(row['ACCT']) in skechers_acct:
			trips.loc[i, 'BREAK3'] = ''
		if row['RECKEY'] in ud05Ids:
			trips.loc[i,'BKAGENT']='ONLINE'
		# [Fs] Eslam request to stop this rule
		# if row['RECKEY'] in ud33ToTrips and row['VALCARR'] != 'ZZ':
		# 	trips.loc[i, 'STNDCHG'] = ud33ToTrips[row['RECKEY']]
		# if row['VALCARRMOD'] == '' or str(row['VALCARRMOD']) == 'nan':
		# 	trips.loc[i, 'VALCARR'] = 'ZZ'
		# 	trips.loc[i, 'STNDCHG'] = ''
		# 	trips.loc[i, 'MKTFARE'] = ''
		# 	trips.loc[i, 'REASCODE'] = ''
		# 	trips.loc[i, 'OFFRDCHG'] = ''
		# 	trips.loc[i, 'AIRCHG'] = ''

		# if row['VALCARR'] == 'ZZ':
		# 	trips.loc[i, 'STNDCHG'] = ''
		# 	trips.loc[i, 'VALCARRMOD'] = ''
		# 	trips.loc[i, 'MKTFARE'] = ''
		# 	trips.loc[i, 'REASCODE'] = ''
		# 	trips.loc[i, 'OFFRDCHG'] = ''
		# 	trips.loc[i, 'AIRCHG'] = ''
		# 	trips.loc[i, 'BASEFARE'] = ''
		# 	trips.loc[i, 'FARETAX'] = ''
		# 	trips.loc[i, 'SVCFEE'] = ''
		# 	trips.loc[i, 'TAX1'] = ''
		# 	trips.loc[i, 'TAX2'] = ''
		# 	trips.loc[i, 'TAX3'] = ''
		# 	trips.loc[i, 'TAX4'] = ''
			# deleted_entries[row['RECKEY']] = 'VALCARR ZZ - NOT A VALID TRIP'
		# print(trips['STNDCHG'])
		# exit()


		if str(row['RECKEY'])[:5] == "17803":
			if "R" or "A" == row['GDS']:
				if "nan" or "" == str(row['VALCARRMOD']):
					trips.loc[i,'VALCARRMOD'] = row['GDS']

	# print(new_airlines)
	for i, row in legs.iterrows():
		if str(row['RECKEY']) in new_airlines:
			legs.loc[i,'AIRLINE'] = new_airlines[str(row['RECKEY'])]

	# The duplicates code has been simplified here as the more elaborate one was causing problems.
	if len(trips) >0:
		trips['PASSLAST'] = trips['PASSLAST'].apply(lambda x:str(x) if 'NEVER' not in str(x).upper() else '')
		trips = trips[trips.PASSLAST != '']

		trips['D1'] = trips.duplicated()
		trips = trips.drop_duplicates()
		
		trips = trips.sort_values(by=['RECKEY','BOOKDATE'])
		trips['EXCHANGE'] = trips['EXCHANGE'].apply(lambda x:'Y' if 'T' == str(x).upper() else x)
		trips['EXCHANGE'] = trips['EXCHANGE'].apply(lambda x:'N' if str(x).upper() == 'F' else x)
		
		legs['D1'] = legs.duplicated()
		legs = legs.drop_duplicates()



	# for i,row in udids.iterrows():
	# 	# if row['RECKEY'][:5] == '33519':
	# 	print(row['UDIDNO'],row['RECKEY'],row['UDIDTEXT'])
	# exit()
	if len(udids) > 0:
		
		ud30s = udids[(udids.UDIDNO == 30)]
		wpb1 = pd.Series(ud30s['UDIDTEXT'].values,index=ud30s.RECKEY).to_dict()

		ud67s = udids[(udids.UDIDNO == 67)]
		wpb2 = pd.Series(ud67s['UDIDTEXT'].values,index=ud67s.RECKEY).to_dict()
		
		ud34s = udids[(udids.UDIDNO == 34)]
		wpb3 = pd.Series(ud34s['UDIDTEXT'].values,index=ud34s.RECKEY).to_dict()

		ud41s = udids[(udids.UDIDNO == 41)]
		wbb2 = pd.Series(ud41s['UDIDTEXT'].values,index=ud41s.RECKEY).to_dict()

		ud28s = udids[(udids.UDIDNO == 28)]		
		wbb3 = pd.Series(ud28s['UDIDTEXT'].values,index=ud28s.RECKEY).to_dict()

		ud230s = udids[(udids.UDIDNO == 230)]
		prophetud = pd.Series(ud230s['UDIDTEXT'].values,index=ud230s.RECKEY).to_dict()
		udids['UDIDNO'] = udids['UDIDNO'].apply(lambda x: 30 if str(x) in ['230','230.0'] else x)
		
		udids = udids.fillna('')
	# for i,row in udids.iterrows():
	# 	print i, row["RECKEY"], row["UDIDNO"], row["UDIDTEXT"]
	# exit("Hello udids END")	
	# udids = udids.drop_duplicates(subset=['RECKEY','UDIDNO'],keep= 'last')
	# udids = df.drop_duplicates(subset=['RECKEY','UDIDNO'],keep= 'last')
	# print udids
	# exit("STOP Its My Time Now")
	try:
		udids = udids.append(udids509)
	except:
		pass
	udids.to_csv('{}\Tcudids.csv'.format(date),index=False)
	udids = pd.read_csv('{}\Tcudids.csv'.format(date))

	#temp dataframe for udidno=32 and udidtext starting with QC
	udid_temp=udids[(udids.UDIDNO == 32)]
	udid_32=udid_temp[udid_temp.UDIDTEXT.str.startswith('QC' , na=False)]

	reckeys_udid32=udid_32['RECKEY'].values.tolist()

	for i,row in udids.iterrows():
		if row['UDIDNO'] == 32 and str(row['UDIDTEXT'])[:2] !='QC':
			if row['RECKEY'] in reckeys_udid32:
				udids.drop(udids.index[i], inplace=True)

	udids = udids.drop_duplicates(['RECKEY','UDIDNO'], keep='first')
	udids.to_csv('{}\Tcudids.csv'.format(date),index=False)

	currency_threshold = pd.read_csv('LUT Currency Threshold.csv')

	currency_codes = pd.Series(currency_threshold['CurrencyCode'].values).to_dict()
	currency_ratio_usd = pd.Series(currency_threshold['Ratio to USD'].values,index=currency_threshold.CurrencyCode).to_dict()
	currency_air_thr = pd.Series(currency_threshold['Air'].values,index=currency_threshold.CurrencyCode).to_dict()
	currency_rail_thr = pd.Series(currency_threshold['Rail'].values,index=currency_threshold.CurrencyCode).to_dict()
	currency_hotel_thr = pd.Series(currency_threshold['Hotel'].values,index=currency_threshold.CurrencyCode).to_dict()
	currency_car_thr = pd.Series(currency_threshold['Car'].values,index=currency_threshold.CurrencyCode).to_dict()



	trips_accts_series = pd.Series(trips['ACCT'].values,index=trips.RECKEY).to_dict()
	trips_invdate_series = pd.Series(trips['INVDATE'].values,index=trips.RECKEY).to_dict()
	trips_recloc_series = pd.Series(trips['RECLOC'].values,index=trips.RECKEY).to_dict()
	trips_passlast_series = pd.Series(trips['PASSLAST'].values,index=trips.RECKEY).to_dict()
	trips_passfrst_series = pd.Series(trips['PASSFRST'].values,index=trips.RECKEY).to_dict()
	trips_bnbr_series = pd.Series(trips['BOOKINGNBR'].values,index=trips.RECKEY).to_dict()


	CurrMaxAudit = []

	thresh_deleted_car = []
	thresh_not_deleted_car = []
	for i,row in cars.iterrows():
		money_type = row['MONEYTYPE']
		if str(money_type) != '' and str(money_type) != 'nan':
			car_threshold = currency_car_thr[money_type]
			ABOOKRAT_con = str(row['ABOOKRAT']).replace('/','') 
			if ABOOKRAT_con != '' and str(ABOOKRAT_con) != 'nan':
				if float(ABOOKRAT_con) > float(car_threshold):
					if len(thresh_deleted_car) > 0:
						thresh_deleted_car = np.append(thresh_deleted_car,row['RECKEY'])
					else:
						thresh_deleted_car = row['RECKEY']
					CurrMaxAudit.append([row['RECKEY'],trips_accts_series[row['RECKEY']],trips_invdate_series[row['RECKEY']],trips_recloc_series[row['RECKEY']],trips_passlast_series[row['RECKEY']],trips_passfrst_series[row['RECKEY']],trips_bnbr_series[row['RECKEY']],row['MONEYTYPE'],'Car',row['ABOOKRAT'],car_threshold])
					cars.drop(i,inplace=True)
				else:
					if len(thresh_not_deleted_car) > 0:
						thresh_not_deleted_car = np.append(thresh_not_deleted_car,row['RECKEY'])
					else:
						thresh_not_deleted_car = row['RECKEY']


	thresh_deleted_hotel = []
	thresh_not_deleted_hotel = []
	for i,row in hotels.iterrows():
		money_type = row['MONEYTYPE']
		if str(money_type) != '' and str(money_type) != 'nan':
			# print(row['RECKEY'])
			hotel_threshold = currency_hotel_thr[money_type]
			BOOKRATE_con = str(row['BOOKRATE']).replace('/','') 
			if BOOKRATE_con != '' and str(BOOKRATE_con) != 'nan':
				if float(BOOKRATE_con) > float(hotel_threshold):
					if len(thresh_deleted_hotel) > 0:
						thresh_deleted_hotel = np.append(thresh_deleted_hotel,row['RECKEY'])
					else:
						thresh_deleted_hotel = row['RECKEY']
					CurrMaxAudit.append([row['RECKEY'],trips_accts_series[row['RECKEY']],trips_invdate_series[row['RECKEY']],trips_recloc_series[row['RECKEY']],trips_passlast_series[row['RECKEY']],trips_passfrst_series[row['RECKEY']],trips_bnbr_series[row['RECKEY']],row['MONEYTYPE'],'Hotel',row['BOOKRATE'],hotel_threshold])
					print(row)
					print(hotels)
					hotels.drop(i,inplace=True)
				else:
					if len(thresh_not_deleted_hotel) > 0:
						thresh_not_deleted_hotel = np.append(thresh_not_deleted_hotel,row['RECKEY'])
					else:
						thresh_not_deleted_hotel = row['RECKEY']


	thresh_deleted_trip = []
	regex = re.compile('[^0-9.:]')
	
	trips.loc[(trips['AIRCHG'].isin(['','nan','NAN'])), 'AIRCHG'] = 0
	trips.loc[(trips['STNDCHG'].isin(['','nan','NAN'])), 'STNDCHG'] = 0
	trips.loc[(trips['OFFRDCHG'].isin(['','nan','NAN'])), 'OFFRDCHG'] = 0

	trips["AIRCHG"] = pd.to_numeric(trips["AIRCHG"])
	trips["STNDCHG"] = pd.to_numeric(trips["STNDCHG"])
	trips["OFFRDCHG"] = pd.to_numeric(trips["OFFRDCHG"])

	trips.loc[trips['AIRCHG'].between(0,100), 'STNDCHG'] = trips['AIRCHG']
	trips.loc[trips['AIRCHG'].between(0,100), 'OFFRDCHG'] = trips['AIRCHG']
	trips.loc[trips['STNDCHG'] < trips['AIRCHG'], 'STNDCHG'] = trips['AIRCHG']
	trips.loc[trips['OFFRDCHG'] > trips['AIRCHG'], 'OFFRDCHG'] = trips['AIRCHG']
	
	for i,row in trips.iterrows():
		# new rules 12_20_2018

		# Yasir Changes
		# if row['AIRCHG'] == '' or row['AIRCHG'] == 'nan':
		# 	row['AIRCHG'] = 0
		# if row['STNDCHG'] == '' or row['STNDCHG'] == 'nan':
		# 	row['STNDCHG'] = 0
		# if row['OFFRDCHG'] == '' or row['OFFRDCHG'] == 'nan':
		# 	row['OFFRDCHG'] = 0
		# if float(row['AIRCHG']) < 100:
		# 	trips.loc[i,'STNDCHG'] = row['AIRCHG']
		# 	trips.loc[i,'OFFRDCHG'] = row['AIRCHG']
		
		# if float(row['STNDCHG']) < float(row['AIRCHG']):
		# 	trips.loc[i,'STNDCHG'] = row['AIRCHG']

		# if float(row['OFFRDCHG']) > float(row['AIRCHG']):
		# 	trips.loc[i,'OFFRDCHG'] = row['AIRCHG']

		# print row['AIRCHG'] , ':' , row['OFFRDCHG']
		# End YASir Changes
		# try:
		# 	if float(row['AIRCHG']) < 100 and float(row['AIRCHG']) > 0 :
		# 		trips.loc[i, 'STNDCHG'] = row['AIRCHG']
		# 		trips.loc[i, 'OFFRDCHG'] = row['AIRCHG']
		# except:
		# 	pass
		# try:
		# 	if float(row['STNDCHG']) < float(row['AIRCHG']):
		# 		trips.loc[i, 'STNDCHG'] = row['AIRCHG']
		# except:
		# 	pass
		# try:
		# 	if float(row['OFFRDCHG']) > float(row['AIRCHG']):
		# 		trips.loc[i, 'OFFRDCHG'] = row['AIRCHG']
		# except:
		# 	pass

		try:
			money_type = row['MONEYTYPE']
			if str(money_type) != '' and str(money_type) != 'nan':
				if str(row['STNDCHG']) != '' and str(row['STNDCHG']) != 'nan':
					STNDCHG_con = regex.sub('',str(row['STNDCHG']))
					if str(STNDCHG_con) != '' and str(STNDCHG_con) != 'nan' and str(STNDCHG_con) != '...' and str(STNDCHG_con) != '..':
						STNDCHG = float(STNDCHG_con) 
					else:
						STNDCHG = 0.0
				else:
					STNDCHG = 0.0
				if str(row['OFFRDCHG']) != '' and str(row['OFFRDCHG']) != 'nan':
					OFFRDCHG_con = regex.sub('',str(row['OFFRDCHG']))
					if str(OFFRDCHG_con) != '' and str(OFFRDCHG_con) != 'nan':
						OFFRDCHG = float(OFFRDCHG_con)
					else:
						OFFRDCHG = 0.0 
				else:
					OFFRDCHG = 0.0
				if str(row['AIRCHG']) != '' and str(row['AIRCHG']) != 'nan':
					AIRCHG_con = regex.sub('',str(row['AIRCHG']))
					if str(AIRCHG_con) != '' and str(AIRCHG_con) != 'nan':
						AIRCHG = float(AIRCHG_con) 
					else:
						AIRCHG = 0.0
				else:
					AIRCHG = 0.0

				if row['VALCARRMOD'] == 'A':
					trip_threshold = currency_air_thr[money_type]
				elif row['VALCARRMOD'] == 'R':
					trip_threshold = currency_rail_thr[money_type]

				if str(row['VALCARRMOD']) != '' and str(row['VALCARRMOD']) != 'nan':
					if row['VALCARR'] == 'ZZ' and (row['RECKEY'] in thresh_deleted_car or row['RECKEY'] in thresh_deleted_hotel) :
						CurrMaxAudit.append([row['RECKEY'],trips_accts_series[row['RECKEY']],trips_invdate_series[row['RECKEY']],trips_recloc_series[row['RECKEY']],trips_passlast_series[row['RECKEY']],trips_passfrst_series[row['RECKEY']],trips_bnbr_series[row['RECKEY']],row['MONEYTYPE'],'',''])
						trips.drop(i,inplace=True)
					elif row['VALCARR'] != 'ZZ' and (row['RECKEY'] in thresh_deleted_car or row['RECKEY'] in thresh_deleted_hotel) and ((STNDCHG > float(trip_threshold)) or (OFFRDCHG > float(trip_threshold)) or (AIRCHG > float(trip_threshold))) :
						if str(row['VALCARRMOD']) == 'A':
							CurrMaxAudit.append([row['RECKEY'],trips_accts_series[row['RECKEY']],trips_invdate_series[row['RECKEY']],trips_recloc_series[row['RECKEY']],trips_passlast_series[row['RECKEY']],trips_passfrst_series[row['RECKEY']],trips_bnbr_series[row['RECKEY']],row['MONEYTYPE'],'A',''])
						elif str(row['VALCARRMOD']) == 'R':
							CurrMaxAudit.append([row['RECKEY'],trips_accts_series[row['RECKEY']],trips_invdate_series[row['RECKEY']],trips_recloc_series[row['RECKEY']],trips_passlast_series[row['RECKEY']],trips_passfrst_series[row['RECKEY']],trips_bnbr_series[row['RECKEY']],row['MONEYTYPE'],'R',''])
						trips.drop(i,inplace=True)
					elif (STNDCHG > float(trip_threshold)) or (OFFRDCHG > float(trip_threshold)) or (AIRCHG > float(trip_threshold)) and (row['RECKEY'] in thresh_not_deleted_car or row['RECKEY'] in thresh_not_deleted_hotel) :
						row['VALCARR'] = 'ZZ'
						row['VALCARRMOD'] = ''
						row['STNDCHG'] = ''
						row['OFFRDCHG'] = ''
						row['REASCODE'] = ''
						row['AIRCHG'] = ''
		except:
			continue



	for i,row in trips.iterrows():
		if row['D1'] == True:
			deleted_records.append([row['RECKEY'],'Trips:Complete Record Matched'])
		# Welbilt UK
		if str(row['RECKEY'])[:5] == '33416':
			if str(row['RECKEY']) in wpb1:
				trips.loc[i,'BREAK1'] = wpb1[str(row['RECKEY'])]
			else:
				trips.loc[i,'BREAK1'] = ''
			if str(row['RECKEY']) in wpb2:
				trips.loc[i,'BREAK2'] = wpb2[str(row['RECKEY'])]
			else:
				trips.loc[i,'BREAK2'] = ''
			if str(row['RECKEY']) in wpb3:
				trips.loc[i,'BREAK3'] = wpb3[str(row['RECKEY'])]
			else:
				trips.loc[i,'BREAK3'] = ''

		# Welbilt Germany
		if str(row['RECKEY'])[:5] == '15916':
			if str(row['RECKEY']) in wbb2:
				if trips.loc[i,'BREAK2'] != wbb2[str(row['RECKEY'])]:
					trips.loc[i,'BREAK2'] = wbb2[str(row['RECKEY'])]
			
			if str(row['RECKEY']) in wbb3:
				if trips.loc[i,'BREAK3'] != wbb3[str(row['RECKEY'])]:
					trips.loc[i,'BREAK3'] = wbb3[str(row['RECKEY'])]


		#Prophet UK
		if str(row['RECKEY'])[:5] == '33519' or str(row['RECKEY'])[:5] == '33719' or str(row['RECKEY'])[:5] == '15019':
			if str(row['RECKEY']) in prophetud:
				if prophetud[str(row['RECKEY'])][:3].lower() == 'bil':
					trips.loc[i,'BREAK1'] = 'BILLABLE'
				elif prophetud[str(row['RECKEY'])][:3].lower() == 'non':
					trips.loc[i,'BREAK1'] = 'NONBILLABLE'
				else:
					trips.loc[i,'BREAK1'] = ''			
##########################################################################################################################
		if str(row['RECKEY'])[:5] == "17803":
			trips.loc[i,'BREAK3'] = row['BREAK1']
			if str(row['VALCARR']) != 'nan' and len(str(row['VALCARR'])) > 2:
				if str(row['VALCARRMOD']) != 'nan' and str(row['VALCARRMOD']) == 'R':
					trips.loc[i,'VALCARR'] = '1R'
				elif str(row['VALCARRMOD']) != 'nan' and str(row['VALCARRMOD']) == 'A':
					trips.loc[i,'VALCARR'] = 'YY'

		

	for i,row in legs.iterrows():
		if 'D1' in trips.columns:
			if row['D1'] == True:
				deleted_records.append([row['RECKEY'],'Legs:Complete Record Matched'])

	if 'D1' in trips.columns:
		del trips['D1']
	if 'D1' in legs.columns:
		del legs['D1']



	CurrMaxAudit = pd.DataFrame(data=CurrMaxAudit,columns=['RECKEY','ACCT','INVDATE','RECLOC','PASSLAST','PASSFRST','BOOKINGNBR','Currency Type','Threshold Type','data record','threshold'])
	AuditZZIncomplete = pd.DataFrame(columns=['RECKEY','INVOICE','INVDATE','ACCT','AGENTID','BRANCH','PSEUDOCITY','BOOKDATE','VALCARR','TICKET','PASSLAST','PASSFRST','STNDCHG','MKTFARE','OFFRDCHG','REASCODE','AIRCHG','BASEFARE','FARETAX','SVCFEE','CREDCARD','CARDNUM','RECLOC','DOMINTL','TRANTYPE','BREAK1','BREAK2','BREAK3','DEPDATE','ARRDATE','PLUSMIN','SAVINGCODE','ACOMMISN','TOURCODE','TICKETTYPE','MONEYTYPE','EXCHANGE','ORIGTICKET','TAX1','TAX2','TAX3','TAX4','IATANBR','TKAGENT','BKAGENT','VALCARRMOD','GDS','TRAVELERID','BOOKINGNBR'])

	if len(legs)>0 and len(hotels)>0 and len(cars)>0 and len(services)>0:
		legs_reckeys = legs['RECKEY'].values
		hotels_reckeys = hotels['RECKEY'].values
		cars_reckeys = cars['RECKEY'].values
		services_reckeys = services['RECKEY'].values	

		for i,row in trips.iterrows():
			if str(row['VALCARR']) == 'ZZ' and (row['RECKEY'] not in legs_reckeys) and (row['RECKEY'] not in hotels_reckeys) and (row['RECKEY'] not in cars_reckeys) and (row['RECKEY'] not in services_reckeys):
				AuditZZIncomplete = AuditZZIncomplete.append(row)
				trips.drop(i,inplace=True)


	print('----------------------------------------------------------------')
	for row in trips.itertuples():
		# print(row.ACCT)
		if row.ACCT == 'WELBILTCOR':
			trips.loc[row.Index,'ACCT'] = 'WELBILTUK1'
			trips.loc[row.Index,'BREAK1'] = 'UK'
	print('----------------------------------------------------------------')
	for row in accounts.itertuples():
		# print(row.ACCT)
		if row.ACCT == 'WELBILTCOR':
			accounts.loc[row.Index,'ACCT'] = 'WELBILTUK1'
			accounts.loc[row.Index,'ACCTNAME'] = 'WELBILT UK LTD'


	accounts.to_csv('{}\Tcaccts.csv'.format(date),index=False)
	trips.to_csv('{}\Tctrips.csv'.format(date),index=False)
	legs.to_csv('{}\Tclegs.csv'.format(date),index=False)
	hotels.to_csv('{}\Tchotel.csv'.format(date),index=False)
	cars.to_csv('{}\Tccars.csv'.format(date),index=False)
	if len(CurrMaxAudit) > 0:
		CurrMaxAudit.to_csv('{}\CurrMaxAudit.csv'.format(date),index=False)
	if len(AuditZZIncomplete) > 0:
		AuditZZIncomplete.to_csv('{}\AuditZZIncomplete.csv'.format(date),index=False)
	# Here the error_report DataFrame is created and written to csv from the array/dictionary we had been using before.

	error_report = pd.DataFrame(data=error_report,columns=['ACCOUNT','TABLE',"RECKEY",'ISSUE'])
	error_report = error_report.drop_duplicates()
	error_report = error_report[error_report.RECKEY != 'RECKEY']

	for i, row in error_report.iterrows():
		if row['ACCOUNT'] == 'TEMP':
			for k,v in country_number_dict.items():
				if v == row['RECKEY'][0:3]:
					country = k
			for k,v in clients_dict.items():
				if v == row['RECKEY'][3:5]:
					client = k
			error_report.loc[i,'ACCOUNT'] = country + ' ' + client
	error_report.to_csv('{}\error_report.csv'.format(date),index=False)

	# Create csv of deleted Records	
	deleted_records = pd.DataFrame(data=deleted_records,columns=['RECKEY','File:Reason'])
	deleted_records.to_csv('{}\DELETED_RECORDS.csv'.format(date),index=False)

	# The directories list is here written to a csv.

	directories = sorted(list(set(directories)))
	with open('{}/directories.csv'.format(date),'w') as f:
		for i in directories:
			line = i + '\n'
			f.write(line)

	# This code writes the list of accounts, with appropriate corresponding data, to a csv.

	account_report = pd.DataFrame(columns=['Account Name','Country','Client','Account Number'])
	trip_accts={}
	if len(trips) >0:
		trip_accts = trips['ACCT'].unique()
	for i in trip_accts:
		df = pd.DataFrame([[acctName_dict[str(i).replace('.0','').lstrip('0')],account_dict[str(i).replace('.0','').lstrip('0')][0],account_dict[str(i).replace('.0','').lstrip('0')][1],str(i).replace('.0','').lstrip('0')]],columns=['Account Name','Country','Client','Account Number'])
		account_report = account_report.append(df)
	account_report = account_report.sort_values(['Country','Account Name'])
	account_report.to_csv('{}\AccountReport.csv'.format(date),index=False)

	# This block of code takes the folder that the processed data has been written to, and moves it to the OUT directory.
	# If a folder of the same name already exists, it is deleted and replaced with the new one.

	dst_file = os.path.join('..\OUT', date)
	if os.path.exists(dst_file):
		rmtree(dst_file)

	# folders = filter( lambda f: not f.startswith('.'), os.walk('.').next()[1])
	folders = [d for d in os.listdir(CurrentDir) if os.path.isdir(d) & (~d.startswith('.'))]
	print("Hello hrFeed-datafile")
	# [fs] read hr feed from the source
	# [fs] we read the file from the source
	# hrFeed = pd.read_csv('hrFeed-datafile.csv')
	global hrFeed
	buCode = pd.read_csv('BU Mapping from Dept.csv')
	hrFeed = hrFeed.fillna('nan')
	ud35_NOHR = {}

	# for i, row in hrFeed.iterrows():
	# 	ud35_NOHR[row['']]



	d1 ={}
	for i in folders:
		for j in glob.glob('{}\*.csv'.format(i)):
			split = j.split('\\')
			directory = split[0].upper()
			tableName = split[-1].replace('.csv', '').strip().upper()
			d1[tableName] = pd.read_csv(j)
	

	macct = d1['ACCOUNTREPORT']
	flag = 0
	flag_non_merck = 0 
	for i, row in macct.iterrows():
		if row['Account Name'] in account_merck:
			flag = 1
	# 	else:
	# 		flag_non_merck = 1
	MerckTrips = []
	
	if len(d1['TCTRIPS'])>0:

		print("Welcom to MERCK Section")

		d1['TCTRIPS']['MERCK/NONMERCK'] = d1['TCTRIPS'].apply(lambda x: 'M' if str(x['ACCT']) in account_num_merck else 'NM',axis=1)

		MerckTrips = d1['TCTRIPS'][d1['TCTRIPS']['MERCK/NONMERCK'] == 'M']
		NonMerckTrips = d1['TCTRIPS'][d1['TCTRIPS']['MERCK/NONMERCK'] == 'NM']

		merck_Reckeys = MerckTrips['RECKEY'].values

		d1['TCUDIDS']['MERCK/NONMERCK'] = d1['TCUDIDS'].apply(lambda x: 'M' if x['RECKEY'] in merck_Reckeys else 'NM', axis=1)

		MerckUdids = d1['TCUDIDS'][d1['TCUDIDS']['MERCK/NONMERCK'] == 'M']
		NonMerckUdids = d1['TCUDIDS'][d1['TCUDIDS']['MERCK/NONMERCK'] == 'NM']
	# print(MerckUdids)
	# print(NonMerckUdids)
	# exit()
	# trips = d1['TCTRIPS']
	# print(len(trips),'11111111111111')
	# if flag_non_merck == 1:	
	# 	Non_Merck_trips = d1['TCTRIPS']
	# Merck_trips_reckey = []
	# for i,row in trips.iterrows():
	# 	if str(row['ACCT']) not in account_no_merck:
	# 		Merck_trips_reckey.append(row['RECKEY'])
	# 		trips = trips.drop(i)
	# 	else:
	# 		if flag_non_merck == 1:
	# 			Non_Merck_trips = Non_Merck_trips.drop(i)
	print("You have crossed 2500 lines :)")


	# udids = d1['TCUDIDS']
	# if flag_non_merck == 1:	
	# 	Non_Merck_udids = d1['TCUDIDS']

	# for i,row in udids.iterrows():
	# 	if row['RECKEY'] in Merck_trips_reckey:
	# 		udids = udids.drop(i)
	# 	else:
	# 		if flag_non_merck == 1:
	# 			Non_Merck_udids = Non_Merck_udids.drop(i)

	# print('-----------------------')
	# print(len(trips))
	# if flag_non_merck == 1:
	# 	print(len(Non_Merck_trips))
	# print('-----------------------')
	# print(len(udids))
	# if flag_non_merck ==1:
	# 	print(len(Non_Merck_udids))
	# print('-----------------------')

	if len(MerckTrips) == 0:
		flag = 0

	# The above code was placed into a function to allow it to be potentially reusable in other files. 
	# This final block of code runs the script automatically when this file is run in Python using "python BatchScript.py".
	if flag != 0:
		# The data is used as the name of the output folder, just like the other scripts on here.
		print('Merck Section')
		date = time.strftime("%m_%d_%Y")

		ud410 = []
		ud411 = []
		ud412 = []
		ud413 = []

		dataHrfeedPopulated = {}
		dataCleared = {}
		popuUsingDiv = {}

		trips = MerckTrips

		udids = MerckUdids

		country_codes = pd.Series(account_list['Country'].values,index=account_list['Acct Number']).to_dict()
		for i,t in trips.iterrows():
			if str(t['BREAK1']) == '' or str(t['BREAK1']) == 'nan':
				trips.loc[i,'BREAK1'] = country_codes[str(t['ACCT'])]
			else:
				print('1')

		udidsHCP = udids[udids.UDIDTEXT == 'HCP QUANTUM']
		ud39_reckeys = []
		for i,row in udidsHCP.iterrows():
			ud39_reckeys.append(row['RECKEY'])
		
		preserve_udids = []
		for i,row in udids.iterrows():
			if row['RECKEY'] in ud39_reckeys:
				preserve_udids.append(row)
				udids.drop(i,inplace=True)
		
		preserve_trips = []
		for i,row in trips.iterrows():
			if row['RECKEY'] in ud39_reckeys:
				preserve_trips.append(row)
				trips.drop(i,inplace=True)

		hotels = d1['TCHOTEL']
		 
		udid_keys = udids['RECKEY'].unique()
		
		# These lines create slices of the udid file with just UD27 or UD28 values, respectively. 
		ud27s = udids[udids.UDIDNO == 27]
		ud28s = udids[udids.UDIDNO == 28]

		ud27ss = pd.DataFrame(columns=['RECKEY', 'UDIDNO', 'UDIDTEXT'])

		traveleridDict = hrFeed['employee_id'].to_dict()
		
		for t,trow in trips.iterrows():
			if str(trow['TRAVELERID']) != 'nan':
				if str(trow['TRAVELERID']) in traveleridDict.values():
					ud27ss.loc[t,'RECKEY'] = trow['RECKEY']
					ud27ss.loc[t,'UDIDNO'] = 27
					ud27ss.loc[t,'UDIDTEXT'] = trow['TRAVELERID']

		if len(ud27ss) > 0:
			ud27s = ud27s.append(ud27ss,ignore_index=True)

		ud27s = ud27s.drop_duplicates()

		# Creates dictionaries mapping a reckey to either a MUID or cost center, respectively.

		id_dict = pd.Series(ud27s['UDIDTEXT'].values,index=ud27s.RECKEY).to_dict()
		bus_dict = pd.Series(ud28s['UDIDTEXT'].values,index=ud28s.RECKEY).to_dict()
		# Creates a bunch of dictionaries mapping a MUID to some value in the HR Feed file.

		lname_dict = pd.Series(hrFeed['last_name'].values,index=hrFeed.employee_id).to_dict()
		fname_dict = pd.Series(hrFeed['first_name'].values,index=hrFeed.employee_id).to_dict()
		ud23s = pd.Series(hrFeed['email_address'].values,index=hrFeed.employee_id).to_dict()
		ud27s = pd.Series(hrFeed['employee_id'].values,index=hrFeed.employee_id).to_dict()
		ud28s = pd.Series(hrFeed['cost_center'].values,index=hrFeed.employee_id).to_dict()

		ud29s = pd.Series(hrFeed['division'].values,index=hrFeed.employee_id).to_dict()
		
		ud28sd = pd.unique(pd.Series(hrFeed['cost_center'].values).dropna())
		ud29sd = pd.unique(pd.Series(hrFeed['division'].values).dropna())
		employees = pd.unique(pd.Series(hrFeed['employee_id'].values).dropna())
		ud32sd = pd.unique(pd.Series(hrFeed['department'].values).dropna())
		ud35sd = pd.unique(pd.Series(hrFeed['business_sector'].values).dropna())

		reckeys = pd.unique(pd.Series(trips['RECKEY'].values).dropna())
		
		ud32s = pd.Series(hrFeed['department'].values,index=hrFeed.employee_id).to_dict()
		ud41s = pd.Series(hrFeed['location_ID'].values,index=hrFeed.employee_id).to_dict()
		ud75s = pd.Series(hrFeed['cmg_number'].values,index=hrFeed.employee_id).to_dict()
		ud96s = pd.Series(hrFeed['manager_email'].values,index=hrFeed.employee_id).to_dict()
		ud35s = pd.Series(hrFeed['business_sector'].values,index=hrFeed.employee_id).to_dict()
		
		ccBU = pd.Series(hrFeed['business_sector'].values,index=hrFeed.cost_center).to_dict()
		ccud29s = pd.Series(hrFeed['division'].values,index=hrFeed.cost_center).to_dict()
		ccud32s = pd.Series(hrFeed['department'].values,index=hrFeed.cost_center).to_dict()
		ccud75s = pd.Series(hrFeed['cmg_number'].values,index=hrFeed.cost_center).to_dict()
		ud29To35 = pd.Series(hrFeed['business_sector'].values,index=hrFeed.division).to_dict()
		buDict = pd.Series(buCode['BU'].values,index=buCode.DEPT).to_dict()
		
		# check = value, val = udidno, reckey = reckeyno
		def appendInDict(check,val,reckey):
			try:
				if str(check) not in ['nan','']:
					if reckey not in dataHrfeedPopulated.keys():
						dataHrfeedPopulated[reckey] = [str(val)]
					elif reckey in dataHrfeedPopulated.keys():
						if str(val) not in dataHrfeedPopulated[reckey]:
							dataHrfeedPopulated[reckey].append(str(val))
			except:
				pass
		
		def appendInClearDict(ud,val,reckey):
			if str(val) not in ['nan','']:
				clearedUD = str(ud) +'/'+ str(val)
				if str(reckey) not in dataCleared.keys():
					dataCleared[str(reckey)] = [str(clearedUD)]
				else:
					if str(clearedUD) not in dataCleared[str(reckey)]:
						dataCleared[str(reckey)].append(str(clearedUD))

		airport_csv = pd.read_csv('Airport_master.csv',encoding='cp1252')
		validB2 = ['HC','LF','LS','MGF','PM']
		trips.loc[~trips['BREAK2'].isin(validB2), 'BREAK2'] = ''
		trips.loc[~trips['BREAK2'].isin(ud29sd), 'BREAK3'] = ''

		for i, row in trips.iterrows():
			
			
			# If BREAK2 value is invalid then set it empty.
			# if row['BREAK2'] not in validB2:
			# 	trips.loc[i,'BREAK2']=''
			# If BREAK3 value is invalid then set it empty.
			# if len(str(row['BREAK3'])) != 2 and len(str(row['BREAK3'])) != 3 or str(row['BREAK3']).isdigit()==True:
			# if row['BREAK3'] not in ud29sd:
			# 	trips.loc[i,'BREAK3']=''
			# Validate TRAVELERID , must not be EMPTY ,start with M or X ,lenght must be 7 or GUEST.	
			if row['TRAVELERID'] != '' and str(row['TRAVELERID']) != 'nan' and (((str(row['TRAVELERID'])[:1] =='M' or str(row['TRAVELERID'])[:1] =='X') and str(row['TRAVELERID'])[1:].isdigit() and len(str(row['TRAVELERID']))==7) or 'GUEST' in str(row['TRAVELERID'])):
				if row['TRAVELERID'] in lname_dict:
					trips.loc[i,'PASSLAST'] = lname_dict[row['TRAVELERID']]
				if row['TRAVELERID'] in fname_dict:
					trips.loc[i,'PASSFRST'] = fname_dict[row['TRAVELERID']]
				if row['TRAVELERID']  in ud29s:
					trips.loc[i,'BREAK3'] = ud29s[row['TRAVELERID']]

				if row['TRAVELERID'] in ud35s:
					trips.loc[i,'BREAK2'] = ud35s[row['TRAVELERID']]
			else:
				if int(row['RECKEY']) in id_dict:
					trips.loc[i,'TRAVELERID'] = id_dict[int(row['RECKEY'])]
					if int(row['RECKEY']) in lname_dict:
						trips.loc[i,'PASSLAST'] = lname_dict[id_dict[int(row['RECKEY'])]]
					if int(row['RECKEY']) in fname_dict:
						trips.loc[i,'PASSFRST'] = fname_dict[id_dict[int(row['RECKEY'])]]
					if id_dict[int(row['RECKEY'])]  in ud29s:
						trips.loc[i,'BREAK3'] = ud29s[id_dict[int(row['RECKEY'])]]
					if id_dict[int(row['RECKEY'])] in ud35s:
						trips.loc[i,'BREAK2'] = ud35s[id_dict[int(row['RECKEY'])]]
				else:		
					if int(row['RECKEY']) in bus_dict:
						if bus_dict[int(row['RECKEY'])] in ccBU:
							trips.loc[i,'BREAK2'] = ccBU[bus_dict[int(row['RECKEY'])]]
							trips.loc[i,'BREAK3'] = ccud29s[bus_dict[int(row['RECKEY'])]]

			# filled=0 
			# if str(row['BREAK3']) not in ud29sd:
			# 	trips.loc[i,'BREAK3'] = ''
			# 	if trips.loc[i,'TRAVELERID'] != '' and str(trips.loc[i,'TRAVELERID']) != 'nan':
			# 		if trips.loc[i,'TRAVELERID']!='GUEST' and trips.loc[i,'TRAVELERID']!='FAMILY':
			# 			if trips.loc[i,'TRAVELERID'] in employees:
			# 				trips.loc[i,'BREAK3'] = ud29s[trips.loc[i,'TRAVELERID']]
			# 				filled=1
			# 		else:
			# 			trips.loc[i,'BREAK3'] = ''
			# 	if filled==0:
			# 		for l, row1 in udids.iterrows():
			# 			if row1['UDIDNO'] == 27 and row1['RECKEY'] in reckeys:
			# 				if udids.loc[l,'RECKEY']==trips.loc[i,'RECKEY']:
			# 					if udids.loc[l,'UDIDTEXT']!='GUEST' and  udids.loc[l,'UDIDTEXT']!='FAMILY':
			# 						if udids.loc[l,'UDIDTEXT'] in employees:
			# 							trips.loc[i,'BREAK3'] = ud29s[udids.loc[l,'UDIDTEXT']]
			# 					else:
			# 						trips.loc[i,'BREAK3'] = ''
		udid_NOHR = {}
		for i, row in udids.iterrows():
			if row['RECKEY'] in id_dict:
				if row['UDIDNO'] == 23:
					if id_dict[int(row['RECKEY'])] in ud23s:
						udids.loc[i,'UDIDTEXT'] = ud23s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud23s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud23s[id_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 27:
					if id_dict[int(row['RECKEY'])] in ud27s:
						udids.loc[i,'UDIDTEXT'] = ud27s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud27s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud27s[id_dict[int(row['RECKEY'])]],row['RECKEY'])
					if str(row['UDIDTEXT']) == "GUEST":
						if [row['RECKEY'],'GUEST'] not in ud410:
							ud410.append([row['RECKEY'],'GUEST'])
					#save reckey of UD27s if they have len 7 and first letter 'X'
					if str(row['UDIDTEXT'])[:1] == "X" and len(row['UDIDTEXT']) == 7:
						if [row['RECKEY'],'XUID'] not in ud410:
							ud410.append([row['RECKEY'],'XUID'])
				if row['UDIDNO'] == 28:
					if id_dict[int(row['RECKEY'])] in ud28s:
						udids.loc[i,'UDIDTEXT'] = ud28s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud28s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud28s[id_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 29:
					if row['UDIDTEXT'] in ud29s.values():
						udids.loc[i,'UDIDTEXT'] = row['UDIDTEXT']
					elif id_dict[int(row['RECKEY'])] in ud29s:
						udids.loc[i,'UDIDTEXT'] = ud29s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud29s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud29s[id_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 32:
					if id_dict[int(row['RECKEY'])] in ud32s:
						udids.loc[i,'UDIDTEXT'] = ud32s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud32s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud32s[id_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 41:
					if id_dict[int(row['RECKEY'])] in ud41s:
						udids.loc[i,'UDIDTEXT'] = ud41s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud41s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud41s[id_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 75:
					if id_dict[int(row['RECKEY'])] in ud75s:
						udids.loc[i,'UDIDTEXT'] = ud75s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud75s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud75s[id_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 96:
					if id_dict[int(row['RECKEY'])] in ud96s:
						udids.loc[i,'UDIDTEXT'] = ud96s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud96s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud96s[id_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 35:
					if id_dict[int(row['RECKEY'])] in ud35s:
						udids.loc[i,'UDIDTEXT'] = ud35s[id_dict[int(row['RECKEY'])]]
						appendInDict(ud35s[id_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ud35s[id_dict[int(row['RECKEY'])]],row['RECKEY'])

			elif row['RECKEY'] in bus_dict:
				if row['UDIDNO'] == 29:
					if bus_dict[int(row['RECKEY'])] in ccud29s:
						udids.loc[i,'UDIDTEXT'] = ccud29s[bus_dict[int(row['RECKEY'])]]
						appendInDict(ccud29s[bus_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ccud29s[bus_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 32:
					if bus_dict[int(row['RECKEY'])] in ccud32s:
						udids.loc[i,'UDIDTEXT'] = ccud32s[bus_dict[int(row['RECKEY'])]]
						appendInDict(ccud32s[bus_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ccud32s[bus_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 75:
					if bus_dict[int(row['RECKEY'])] in ccud75s:
						udids.loc[i,'UDIDTEXT'] = ccud75s[bus_dict[int(row['RECKEY'])]]
						appendInDict(ccud75s[bus_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ccud75s[bus_dict[int(row['RECKEY'])]],row['RECKEY'])
				if row['UDIDNO'] == 35:
					if bus_dict[int(row['RECKEY'])] in ccBU:
						udids.loc[i,'UDIDTEXT'] = ccBU[bus_dict[int(row['RECKEY'])]]
						appendInDict(ccBU[bus_dict[int(row['RECKEY'])]],row['UDIDNO'],str(row['RECKEY']))
						# appendTo412(ccBU[bus_dict[int(row['RECKEY'])]],row['RECKEY'])

		for k,v in id_dict.items():
			temp = udids[udids.RECKEY == k]
			if 23 not in temp['UDIDNO'].unique():
				if v in ud23s:
					load = pd.DataFrame([[k,23,ud23s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ud23s[v],23,str(k))
					# appendTo412(ud23s[v],k)
			if 27 not in temp['UDIDNO'].unique():
				if v in ud27s:
					load = pd.DataFrame([[k,27,ud27s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					# appendInDict(ud27s[v],27,str(k))
					# appendTo412(ud27s[v],k)
			if 28 not in temp['UDIDNO'].unique():
				if v in ud28s:
					load = pd.DataFrame([[k,28,ud28s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ud28s[v],28,str(k))
					# appendTo412(ud28s[v],k)
			if 29 not in temp['UDIDNO'].unique():
				if v in ud29s:
					load = pd.DataFrame([[k,29,ud29s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ud29s[v],29,str(k))
					# appendTo412(ud29s[v],k)
				# elif v in :
			if 32 not in temp['UDIDNO'].unique():
				if v in ud32s:
					load = pd.DataFrame([[k,32,ud32s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ud32s[v],32,str(k))
					# appendTo412(ud32s[v],k)
			if 41 not in temp['UDIDNO'].unique():
				if v in ud41s:
					load = pd.DataFrame([[k,41,ud41s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ud41s[v],41,str(k))
					# appendTo412(ud41s[v],k)
			if 75 not in temp['UDIDNO'].unique():
				if v in ud75s:
					load = pd.DataFrame([[k,75,ud75s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ud75s[v],75,str(k))
					# appendTo412(ud75s[v],k)
			if 96 not in temp['UDIDNO'].unique():
				if v in ud96s:
					load = pd.DataFrame([[k,96,ud96s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ud96s[v],96,str(k))
					# appendTo412(ud96s[v],k)
			if 35 not in temp['UDIDNO'].unique():
				if v in ud35s:
					load = pd.DataFrame([[k,35,ud35s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ud35s[v],35,str(k))
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
					# appendTo412(ud35s[v],k)
		
		for k,v in bus_dict.items():
			temp = udids[udids.RECKEY == k]
			if 29 not in temp['UDIDNO'].unique():
				if v in ccud29s:
					load = pd.DataFrame([[k,29,ccud29s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ccud29s[v],29,str(k))
					# appendTo412(ccud29s[v],k)
			if 32 not in temp['UDIDNO'].unique():
				if v in ccud32s:
					load = pd.DataFrame([[k,32,ccud32s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ccud32s[v],32,str(k))
					# appendTo412(ccud32s[v],k)
			if 75 not in temp['UDIDNO'].unique():
				if v in ccud75s:
					load = pd.DataFrame([[k,75,ccud75s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ccud75s[v],75,str(k))
					# appendTo412(ccud75s[v],k)
			if 35 not in temp['UDIDNO'].unique():
				if v in ccBU:
					load = pd.DataFrame([[k,35,ccBU[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
					appendInDict(ccBU[v],35,str(k))
					# appendTo412(ccBU[v],k)

		udids = udids.reset_index()
		copyUdids=udids
		for i, row in udids.iterrows():
			if row['UDIDNO'] == 29 and row['RECKEY'] in reckeys:
				if udids.loc[i,'UDIDTEXT'] not in ud29sd:
					travelerR = trips[trips.RECKEY == row['RECKEY']]
					if travelerR['TRAVELERID'].values[0]=='GUEST' or travelerR['TRAVELERID'].values[0]=='FAMILY':
						appendInClearDict(row['UDIDNO'],row['UDIDTEXT'],row['RECKEY'])
						udids.loc[i,'UDIDTEXT']=''
					elif travelerR['TRAVELERID'].values[0] in employees:
						if travelerR['TRAVELERID'].values[0] in ud29s:
							udids.loc[i,'UDIDTEXT'] = ud29s[travelerR['TRAVELERID'].values[0]]
							appendInDict(ud29s[travelerR['TRAVELERID'].values[0]],row['UDIDNO'],str(row['RECKEY']))
							# appendTo412(ud29s[travelerR['TRAVELERID'].values[0]],str(row['RECKEY']))	
					else:
						for l,row1 in udids.iterrows():
							if row1['UDIDNO'] == 27 and row1['RECKEY'] in reckeys:
								if udids.loc[l,'RECKEY']==udids.loc[i,'RECKEY']:
									if udids.loc[l,'UDIDTEXT']!='GUEST' and udids.loc[l,'UDIDTEXT']!='FAMILY' : 
										if udids.loc[l,'UDIDTEXT'] in employees:
											if row1['UDIDTEXT'] in ud29s:
												udids.loc[i,'UDIDTEXT'] = ud29s[row1['UDIDTEXT']]
												# appendInDict(ud29s[row1['UDIDTEXT']],row['UDIDNO'],str(row['RECKEY']))
												# appendTo412(ud29s[row1['UDIDTEXT']],str(row['RECKEY']))
									else:
										appendInClearDict(row1['UDIDNO'],row1['UDIDTEXT'],row1['RECKEY'])
										udids.loc[i,'UDIDTEXT']=''
			# remove UDIDTEXT of UD35s,UD32s,UD28s,UD29s and UD27s if they are invalid
			if row['UDIDNO'] == 35:
				if row['UDIDTEXT'] not in list(buCode['BU']):
					appendInClearDict(row['UDIDNO'],row['UDIDTEXT'],row['RECKEY'])
					udids.loc[i,'UDIDTEXT'] = ''
			if row['UDIDNO'] == 27:
				if row['UDIDTEXT'] not in employees:
					appendInClearDict(row['UDIDNO'],row['UDIDTEXT'],row['RECKEY'])
					udids.loc[i,'UDIDTEXT'] = ''
			if row['UDIDNO'] == 28:
				if row['UDIDTEXT'] not in ud28sd:
					appendInClearDict(row['UDIDNO'],row['UDIDTEXT'],row['RECKEY'])
					udids.loc[i,'UDIDTEXT'] = ''
			if row['UDIDNO'] == 29:
				if row['UDIDTEXT'] not in ud29sd:
					appendInClearDict(row['UDIDNO'],row['UDIDTEXT'],row['RECKEY'])
					udids.loc[i,'UDIDTEXT'] = ''
			if row['UDIDNO'] == 32:
				if row['UDIDTEXT'] not in ud32sd and str(row['UDIDTEXT'])[:2] != 'QC':
					appendInClearDict(row['UDIDNO'],row['UDIDTEXT'],row['RECKEY'])
					udids.loc[i,'UDIDTEXT'] = ''
 
		break2_toFill = {}
		
		for i,row in udids.iterrows():
			#populate empty ud35s with ud32s first two characters
			if row['UDIDNO'] == 35:
				if str(row['UDIDTEXT']) == 'nan' or row['UDIDTEXT'] == '':
					if int(row['RECKEY']) in id_dict.keys():
						if id_dict[int(row['RECKEY'])] in ud32s:
							if str(ud32s[id_dict[int(row['RECKEY'])]])[:2] in buDict:
								# print str(ud32s[id_dict[int(row['RECKEY'])]])[:2],buDict[str(ud32s[id_dict[int(row['RECKEY'])]])[:2]]
								udids.loc[i,'UDIDTEXT'] = buDict[str(ud32s[id_dict[int(row['RECKEY'])]])[:2]]
								break2_toFill[row['RECKEY']] = buDict[str(ud32s[id_dict[int(row['RECKEY'])]])[:2]]

		udids = udids.drop('index',1)
		
		# These lines write the updated trips and udids files back into the dictionary containing the 7 files, overwriting the old values.
	
		trips['EXCHANGE'] = trips['EXCHANGE'].apply(lambda x:'Y' if 'T' == str(x).upper() else x)
		trips['EXCHANGE'] = trips['EXCHANGE'].apply(lambda x:'N' if str(x).upper() == 'F' else x)
		trips['PASSLAST'] = trips['PASSLAST'].apply(lambda x:str(x) if 'NEVER' not in str(x).upper() else '')
		trips = trips[trips.PASSLAST != '']
	
		for i,row in trips.iterrows():

			if row['TRAVELERID'] != '' and str(row['TRAVELERID']) != 'nan' and (((str(row['TRAVELERID'])[:1] =='M' or str(row['TRAVELERID'])[:1] =='X') and str(row['TRAVELERID'])[1:].isdigit() and len(str(row['TRAVELERID']))==7)): #or 'GUEST' in str(row['TRAVELERID'])):

				if str(row['BREAK3']) == '' or str(row['BREAK3']) == 'nan':
					
					if row['TRAVELERID'] in ud32s:
						
						trips.loc[i,'BREAK3'] = str(ud32s[row['TRAVELERID']])[0:2]
			else:
				if str(row['BREAK3']) == '' or str(row['BREAK3']) == 'nan':
					if row['TRAVELERID'] in ud32s:
						trips.loc[i,'BREAK3'] = str(ud32s[id_dict[int(row['RECKEY'])]])[0:2]
			
			if str(row['ACCT']) in skechers_acct:
				appendInClearDict(29,row['BREAK3'],row['RECKEY'])
				trips.loc[i, 'BREAK3'] = ''
			
			if str(row['BREAK2']) == '' or str(row['BREAK2']) == 'nan':
				if row['RECKEY'] in break2_toFill or str(row['RECKEY']) in break2_toFill:
					trips.loc[i,'BREAK2'] = break2_toFill[row['RECKEY']]

			#save reckey of trips if row travelerid have len 6 and first letter 'X'
			if str(row['TRAVELERID'])[:1] == "X" and len(row['TRAVELERID']) == 7:
				if [row['RECKEY'],'XUID'] not in ud410:
					ud410.append([row['RECKEY'],'XUID'])
			if str(row['TRAVELERID']) == "GUEST":
				if [row['RECKEY'],'GUEST'] not in ud410:
					ud410.append([row['RECKEY'],'GUEST'])
			
			if row['TRAVELERID'] not in employees:
				appendInClearDict(27,row['TRAVELERID'],row['RECKEY'])
				trips.loc[i,'TRAVELERID'] = ''
			if row['BREAK3'] not in ud29sd:
				appendInClearDict(29,row['BREAK3'],row['RECKEY'])
				trips.loc[i,'BREAK3'] = ''
			if row['BREAK2'] not in ud35sd:
				appendInClearDict(35,row['BREAK2'],row['RECKEY'])
				trips.loc[i,'BREAK2'] = ''

		# This section fills empty TRIPS TRAVELERID,BREAK2 and BREAK3 and UDIDS UDIDTEXT.
		# Populates new UDs in UDIDS
		
		f_l_name = pd.DataFrame(columns=['fname','lname','employee_id','country'])
		
		# stores all first_name,last_name and employees from Hrfeed in f_l_name dataframe
		f_l_name['fname'] = hrFeed['first_name'].apply(lambda x: x.split('-',1)[0].upper() if x.find('-') != -1 else x.upper())
		f_l_name['lname'] = hrFeed['last_name'].apply(lambda x: x.split('-',1)[0].upper() if x.find('-') != -1 else x.upper())
		f_l_name['employee_id'] = hrFeed['employee_id'].apply(lambda x: x)
		f_l_name['country'] = hrFeed['work_country'].apply(lambda x: x)

		# This dataframe includes rows that has no travelerId 
		empty_tid = trips[(trips.TRAVELERID == '') | (str(trips.TRAVELERID) == 'nan')]
		print("You have crossed 3000 lines :)"	)

		# this iteration fetch data from f_l_name and append in tid_to_fill array that has matching fname and lname
		# and matches them with empty_tid PASSFRST and PASSLAST
		tid_to_fill = []
		for i,row in empty_tid.iterrows():
			data = f_l_name[(f_l_name['fname'] == row['PASSFRST']) & (f_l_name['lname'] == row['PASSLAST']) & (f_l_name['country'] == row['BREAK1'])]
			if len(data) > 0:
				tid_to_fill.append(data)

		# populate trips Break2,Break3 and Travelerid using tid_to_fill array and append employee_id as value and
		# reckey as key in rows_were_filled dictionary that will be used to populate UDs or UDIDTEXT in UDIDS 
		rows_were_filled = {}
		for v in tid_to_fill:
			if len(v) != 0:
				for i,row in trips.iterrows():
					if row['PASSFRST'] == (v['fname'].values)[0] and row['PASSLAST'] == (v['lname'].values)[0] and (row['BREAK1'] == (v['country'].values)[0]):
						trips.loc[i,'TRAVELERID'] =  (v['employee_id'].values)[0]
						rows_were_filled[row['RECKEY']] = (v['employee_id'].values)[0]
						if row['BREAK3'] == '' and (v['employee_id'].values)[0] in ud29s:
							trips.loc[i,'BREAK3'] = ud29s[(v['employee_id'].values)[0]]
						if row['BREAK2'] == '' and (v['employee_id'].values)[0] in ud35s:
							trips.loc[i,'BREAK2'] = ud35s[(v['employee_id'].values)[0]]
						# print (v['employee_id'].values)[0],ud29s[(v['employee_id'].values)[0]],ud35s[(v['employee_id'].values)[0]]
		# print len(rows_were_filled)

		# this iteration populated new UDs that are not in UDIDs but has information in TRIPS
		for k,v in rows_were_filled.items():
			temp = udids[udids.RECKEY == k]
			if 27 not in temp['UDIDNO'].unique():
				appendInDict(v,27,str(k))
				load = pd.DataFrame([[k,27,v]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
				udids = udids.append(load)
				if str(v) == "GUEST":
					if [k,'GUEST'] not in ud410:
						ud410.append([k,'GUEST'])
				#save reckey of UD27s if they have len 7 and first letter 'X'
				if str(v)[:1] == "X" and len(v) == 7:
					if [k,'XUID'] not in ud410:
						ud410.append([k,'XUID'])
			if 29 not in temp['UDIDNO'].unique():
				if v in ud29s:
					appendInDict(v,29,str(k))
					load = pd.DataFrame([[k,29,ud29s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)
			if 35 not in temp['UDIDNO'].unique():
				if v in ud35s:
					appendInDict(v,35,str(k))
					load = pd.DataFrame([[k,35,ud35s[v]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
					udids = udids.append(load)

		# this iteration populates UDIDTEXT that was added in TRIPS Break2,break3 & Travelerid
		for i,row in udids.iterrows():
			if row['UDIDNO'] == 27 and row['UDIDTEXT'] == '':
				if row['RECKEY'] in rows_were_filled:
					udids.loc[i,'UDIDTEXT'] = rows_were_filled[row['RECKEY']]
					appendInDict(rows_were_filled[row['RECKEY']],row['UDIDNO'],str(row['RECKEY']))
					# appendTo412(rows_were_filled[row['RECKEY']],row['RECKEY'])
			if row['UDIDNO'] == 29 and row['UDIDTEXT'] == '':
				if row['RECKEY'] in rows_were_filled:
					udids.loc[i,'UDIDTEXT'] = ud29s[rows_were_filled[row['RECKEY']]]
					appendInDict(ud29s[rows_were_filled[row['RECKEY']]],row['UDIDNO'],str(row['RECKEY']))
					# appendTo412(ud29s[rows_were_filled[row['RECKEY']]],row['RECKEY'])
			if row['UDIDNO'] == 35 and row['UDIDTEXT'] == '':
				if row['RECKEY'] in rows_were_filled:
					udids.loc[i,'UDIDTEXT'] = ud35s[rows_were_filled[row['RECKEY']]]
					appendInDict(ud35s[rows_were_filled[row['RECKEY']]],row['UDIDNO'],str(row['RECKEY']))
					# appendTo412(ud35s[rows_were_filled[row['RECKEY']]],row['RECKEY']

		# this section populates value from trips to udids and add new reckey if they are not in udids
		emptyUDs = udids[((udids.UDIDNO == 29) | (udids.UDIDNO == 35)) & (udids.UDIDTEXT == '')]
		
		b2_dict = pd.Series(trips['BREAK2'].values,index=trips.RECKEY).to_dict()
		b3_dict = pd.Series(trips['BREAK3'].values,index=trips.RECKEY).to_dict()

		for i,row in emptyUDs.iterrows():
			if row['RECKEY'] in reckeys:
				if row['RECKEY'] in b2_dict:
					if b2_dict[row['RECKEY']] not in ['nan','']:
						udids.loc[i,'UDIDTEXT'] = b2_dict[row['RECKEY']]

		for i in reckeys:
			temp = udids[udids.RECKEY == i]
			if 35 not in temp['UDIDNO'].unique():
				if i in b2_dict:
					if b2_dict[i] not in ['nan','']:
						# appendInDict(b2_dict[i],35,str(i))
						load = pd.DataFrame([[i,35,b2_dict[i]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
						udids = udids.append(load)
		
		b2z = trips[trips.BREAK2 == '']
		if len(b2z) > 0:
			ud35z = udids[udids.UDIDNO == 35]
			ud35zDict = pd.Series(ud35z['UDIDTEXT'].values,index=ud35z.RECKEY).to_dict()
			for i,row in b2z.iterrows():
				if row['RECKEY'] in ud35zDict:
					if ud35zDict[row['RECKEY']] not in ['nan','']:
						trips.loc[i,'BREAK2'] = ud35zDict[row['RECKEY']]
		# populate Break2 and Break3 using UD35s and UD29s [line 2175-2190]
		b3z = trips[trips.BREAK3 == '']
		if len(b3z) > 0:
			ud29z = udids[udids.UDIDNO == 29]
			ud29zDict = pd.Series(ud29z['UDIDTEXT'].values,index=ud29z.RECKEY).to_dict()
			for i,row in b3z.iterrows():
				if row['RECKEY'] in ud29zDict:
					if ud29zDict[row['RECKEY']] not in ['nan','']:
						trips.loc[i,'BREAK3'] = ud29zDict[row['RECKEY']]

		# Using BREAK3 BU will be fetched from HRfeed file and then populated in BREAK2
		for i,row in trips.iterrows():
			if row['BREAK3'] not in ['nan',''] and row['BREAK2'] in ['nan','']:
				if row['BREAK3'] in ud29To35:
					if ud29To35[row['BREAK3']] not in ['nan','']:
						trips.loc[i,'BREAK2'] = ud29To35[row['BREAK3']]
						popuUsingDiv[row['RECKEY']] = ud29To35[row['BREAK3']]
			if row['BREAK2'] not in validB2:
				trips.loc[i,'BREAK2'] = 'UNK'

		# populated value of Break2 will be populated in UDIDs with Reckey
		for k,v in popuUsingDiv.items():
			temp = udids[udids.RECKEY == k]
			if 35 not in temp['UDIDNO'].unique():
				load = pd.DataFrame([[k,35,v]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
				udids = udids.append(load)
				appendInDict(v,35,str(k))

		# populated value of Break2 will be populated in UDIDs UDIDTEXT
		for i,row in udids.iterrows():
			if row['RECKEY'] in popuUsingDiv and row['UDIDNO'] == 35 and str(row['UDIDTEXT']) in ['nan','']:
				udids.loc[i,'UDIDTEXT'] = popuUsingDiv[row['RECKEY']]
				appendInDict(popuUsingDiv[row['RECKEY']],row['UDIDNO'],str(row['RECKEY']))

		NOHR_data = {}
		ud27_pre_NOHR = udids[udids.UDIDNO == 27]

		def detect_nohr(r,udn,udt):
			if str(udt) != '' and str(udt) != 'nan':
				if udt in ud23s:
					if str(ud23s[udt]) == 'nan' or str(ud23s[udt]) == '':
						if r in NOHR_data:
							NOHR_data[int(r)].append('23')
						else:
							NOHR_data[int(r)] = ['23'] 
						
				if udt in ud27s:
					if str(ud27s[udt]) == 'nan' or str(ud27s[udt]) == '':
						if r in NOHR_data:
							NOHR_data[int(r)].append('27')
						else:
							NOHR_data[int(r)] = ['27']
				if udt in ud28s:
					if str(ud28s[udt]) == 'nan' or str(ud28s[udt]) == '':
						if r in NOHR_data:
							NOHR_data[int(r)].append('28')
						else:
							NOHR_data[int(r)] = ['28']
				if udt in ud29s:
					if str(ud29s[udt]) == 'nan' or str(ud29s[udt]) == '':
						if r in NOHR_data:
							NOHR_data[int(r)].append('29')
						else:
							NOHR_data[int(r)] = ['29']
				if udt in ud32s:
					if str(ud32s[udt]) == 'nan' or str(ud32s[udt]) == '':
						if r in NOHR_data:
							NOHR_data[int(r)].append('32')
						else:
							NOHR_data[int(r)] = ['32']
				if udt in ud35s:
					if str(ud35s[udt]) == 'nan' or str(ud35s[udt]) == '':
						if str(udt) == 'M179752':
							print(ud35s[udt],'1111111111')
						if r in NOHR_data:
							NOHR_data[int(r)].append('35')
						else:
							NOHR_data[int(r)] = ['35']
				if udt in ud41s:
					if str(ud41s[udt]) == 'nan' or str(ud41s[udt]) == '':
						if r in NOHR_data:
							NOHR_data[int(r)].append('41')
						else:
							NOHR_data[int(r)] = ['41']
				if udt in ud75s:
					if str(ud75s[udt]) == 'nan' or str(ud75s[udt]) == '':
						if r in NOHR_data:
							NOHR_data[int(r)].append('75')
						else:
							NOHR_data[int(r)] = ['75']
				if udt in ud96s:
					if str(ud96s[udt]) == 'nan' or str(ud96s[udt]) == '':
						if r in NOHR_data:
							NOHR_data[int(r)].append('96')
						else:
							NOHR_data[int(r)] = ['96']

		# print('-----------------------------------')
		# print(str(ud35s['M179752']) == '',str(ud35s['M179752']) == 'nan')
		# print('-----------------------------------')
		for i, row in ud27_pre_NOHR.iterrows():
			detect_nohr(row['RECKEY'],row['UDIDNO'],row['UDIDTEXT'])
		# print(NOHR_data)
		
		# for i,row in udids.iterrows():
		# 	if row['RECKEY'] in udid_NOHR:
		# 		if row['UDIDNO'] == udid_NOHR[int(row['RECKEY'])]:
		# 			udids.loc[i,'UDIDTEXT'] = 'NOHR'

		def set_nohr(r,un,ut,typee):
			if r in NOHR_data:
				# print(typee,r,un,111111111)
				if str(int(un)) in NOHR_data[r]:
					# print(typee,r,un,222222222)
					return 'NOHR'
				# elif str(ut) == 'BREAK2' or str(ut) == 'BREAK3' :
				# 	return 'NOHR'
			return ut

		udids['UDIDTEXT'] = udids.apply(lambda x: set_nohr(x['RECKEY'],x['UDIDNO'],x['UDIDTEXT'],'udid'),axis=1)
		# print(udid_NOHR)

		# udid_nan = udids[udids['UDIDTEXT'] == 'nan']
		# print(udid_nan)
		# for i,row in udid_nan.iterrows():
		# 	print(row['RECKEY'])
		# 	print(row['RECKEY'] in id_dict)
		# 	print(row['RECKEY'] in bus_dict)




		# This loop convert each list into a string which is stored against RECKEYs
		# For Example : '1234':-->['23','27','29']<-- will be converted to 'REF HR 23 27 29'
		# Pass Reckeys and Generated Text which is stored in ud411Text to ud411 array
		for k,v in dataHrfeedPopulated.items():
			
			ud411Text = "REF HR "
			ud411Text+= ' '.join(v)
			ud411.append([k,ud411Text])

		# Add UD410 in UDIDS
		for i in ud410:
			df = pd.DataFrame([[i[0],'410',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
			udids = udids.append(df)
		# populated UD411 in UDIDS
		for i in ud411:
			df = pd.DataFrame([[i[0],'411',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
			udids = udids.append(df)
		# populated UD412 in UDIDS but it will check that the same reckey is not available for UD411s
		ud411ReckeySet = [item[0] for item in ud411]
		for i in ud412:
			if i[0] not in ud411ReckeySet: 
				df = pd.DataFrame([[i[0],'412','NO HR DATA']],columns=['RECKEY','UDIDNO','UDIDTEXT'])
				udids = udids.append(df)

		for k,v in dataCleared.items():
			
			ud413Text = "CLEAR "
			ud413Text+= ' '.join(v)
			ud413.append([k,ud413Text])
		
		# populated UD413 in UDIDS
		for i in ud413:
			df = pd.DataFrame([[i[0],'413',i[-1]]],columns=['RECKEY','UDIDNO','UDIDTEXT'])
			udids = udids.append(df)

		# emptyTraveler = trips[(trips.TRAVELERID == '') | (trips.TRAVELERID == 'nan')]
		# print len(emptyTraveler)
		# ud27z = udids[udids.UDIDNO == 27]
		# print len(ud27z)
		# ud27zDict = pd.Series(ud27z['UDIDTEXT'].values,index=ud27z.RECKEY).to_dict()

		# for i,row in emptyTraveler.iterrows():
		# 	if row['RECKEY'] in ud27zDict:
		# 		print 'po'
		# 		trips.loc[i,'TRAVELERID'] = ud27zDict[row['RECKEY']] 
		# populate UNK in trips Break2 if they are empty
		# eb2 = trips[(trips.BREAK2 != 'HC') & (trips.BREAK2 != 'LF') & (trips.BREAK2 != 'LS') & (trips.BREAK2 != 'MGF') & (trips.BREAK2 != 'PM')]
		# for i,row in eb2.iterrows():
		# 	trips.loc[i,'BREAK2'] = 'UNK'

		if len(preserve_trips) > 0:
			trips = trips.append(preserve_trips,ignore_index=True)
		if len(preserve_udids) > 0:
			udids = udids.append(preserve_udids,ignore_index=True)

		for i,row in trips.iterrows():
			if str(row['BREAK3']) == '' or str(row['BREAK3']) == 'nan':
				trips.loc[i,'BREAK3'] = 'UNK'
			if set_nohr(row['RECKEY'],35,row['BREAK2'],'break') == 'NOHR':
				trips.loc[i,'BREAK2'] = 'NOHR'
			if set_nohr(row['RECKEY'],29,row['BREAK3'],'break') == 'NOHR':
				trips.loc[i,'BREAK3'] = 'NOHR'


		def update_nan(r,un,udtext):
			if str(udtext) == 'nan' or str(udtext) == '':
				return 'UNK'
			else:
				return udtext


		udids['UDIDTEXT'] = udids.apply(lambda x: update_nan(x['RECKEY'],x['UDIDNO'],x['UDIDTEXT']),axis=1)

		# if flag_non_merck == 1 and flag == 1:
		trips = trips.append(NonMerckTrips, sort = False)
		udids = udids.append(NonMerckUdids, sort = False)

		udids = udids.reindex(columns=['RECKEY','UDIDNO','UDIDTEXT'])
		trips = trips.reindex(columns=['RECKEY','INVOICE','INVDATE','ACCT','AGENTID','BRANCH','PSEUDOCITY','BOOKDATE','VALCARR','TICKET','PASSLAST','PASSFRST','STNDCHG','MKTFARE','OFFRDCHG','REASCODE','AIRCHG','BASEFARE','FARETAX','SVCFEE','CREDCARD','CARDNUM','RECLOC','DOMINTL','TRANTYPE','BREAK1','BREAK2','BREAK3','DEPDATE','ARRDATE','PLUSMIN','SAVINGCODE','ACOMMISN','TOURCODE','TICKETTYPE','MONEYTYPE','EXCHANGE','ORIGTICKET','TAX1','TAX2','TAX3','TAX4','IATANBR','TKAGENT','BKAGENT','VALCARRMOD','GDS','TRAVELERID','BOOKINGNBR'])

		trips = trips.drop_duplicates()
		udids = udids.drop_duplicates()
		d1['TCTRIPS'] = trips
		# udids = udids.append(udidsHCP,ignore_index=True)
		# print(udids)
		# exit()
		d1['TCUDIDS'] = udids

		# Creates a directory in the folder with the date the script was run.
		rmtree('%s' %date)
		
		os.mkdir('%s' % date)

		# Writes the data into that new directory. I was ignoring svcfees records when working with some of the test data, but I removed that piece of code for being
		# superfluous now. If you want to add it back in, just copy the version of this block that you sent me on Friday in over these two lines.

		print('----------------------------------------------------------------','triple1')
		for row in d1['TCTRIPS'].itertuples():
			# print(row.ACCT)
			if row.ACCT == 'WELBILTCOR':
				d1['TCTRIPS'].loc[row.Index,'ACCT'] = 'WELBILTUK1'
				d1['TCTRIPS'].loc[row.Index,'BREAK1'] = 'UK'
		print('----------------------------------------------------------------','triple2')
		for row in d1['TCACCTS'].itertuples():
			# print(row.ACCT)
			if row.ACCT == 'WELBILTCOR':
				d1['TCACCTS'].loc[row.Index,'ACCT'] = 'WELBILTUK1'
				d1['TCACCTS'].loc[row.Index,'ACCTNAME'] = 'WELBILT UK LTD'


		for name, df in d1.items():
			df.to_csv('{0}\\{1}.csv'.format(date,name),index=False)

		# Moves the directory to the outfolder, deleting any folder with the same name that is already in there. This is why you need to be sure to run one at a time,
		# and rename the folder to whatever you need it to be, before running another one on the same day.
		dst_file = os.path.join('..\OUT', date)
		
		# if os.path.exists(dst_file):
		# 	rmtree(dst_file)

		move('%s' % date,'..\OUT')
		end_time = time.time()
		print(end_time-start_time)
		print(":) Good Luck and wish you for the best Bye Bye :)")			
	else:
		print(":) Good Luck and wish you for the best Bye Bye :)")	
		move(date,'..\OUT')
		###############################################################
first_day_of_the_month = datetime.date.today().replace(day=1).strftime("%d-%b-%Y")
current_month = datetime.date.today().strftime("%m")
last_day_prev_month = date.today().replace(day=1) - timedelta(days=1) 
prev_month = last_day_prev_month.strftime('%b,%Y')

if __name__ == "__main__":
	

	master_script()
	con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
					"Server=sql-looker-db.database.windows.net;"
						"Database=Looker_live;" 
						"uid=atg-admin;pwd=Travel@123;")
	cursor = con.cursor()

	Monthly_BO_data_details = pd.read_sql_query('select * from Monthly_BO_data_details', con)
	print(Monthly_BO_data_details.to_string(index=False))

	for i, row in Monthly_BO_data_details.iterrows():
		agency_id = (row['agency_id'])
		# print(agency_id)

		if value == row['agency_id']:
			# print('match')
			# print(agency_id)
			email_received = row['email_received']
			files_received = row['files_received']
			processed = row['processed']
			data_of_month = row['data_of_month']
			date =row['date']
			created_at = row['created_at']
			# print(date)
			# print(data_of_month)
			# print(processed)
			# exit()
			if data_of_month == prev_month and Yes_No in ('Y','y'):
				processed = 1
				print('date matched')

				print("""UPDATE Monthly_BO_data_details SET  processed=? WHERE agency_id=? and data_of_month=?""",(processed, agency_id, data_of_month))

				cursor.execute("""
				UPDATE Monthly_BO_data_details SET  processed=? WHERE agency_id=? and data_of_month=?
				""", (processed, agency_id, data_of_month))

				# con.commit()
				print('updated successfully')
				exit()
			elif data_of_month == prev_month and (Yes_No in ('N','n') or value == 0):
				print("Nothing to be Updated.")
				exit()
			else:
				print("Nothing to be Updated.")

	print('LUT Global Account' ,account_list.tail())
	print(FileName , ' >>> HRFEED')
	print("You have Done all work, Congratulations.", "\U0001F917")