'''
This script reads the CIP-API, identifies any probands which have been sent to the GMC for review (using the status field) \
and will enable these to be imported into moka.
'''
import requests
import pyodbc
import json
import datetime
import sys
import getopt

# Import local settings
from authentication import APIAuthentication # import the function from the authentication script which generates the access token
from database_connection_config import * # database connection details
from gel_report_config import * # config file 


class connect():
	def __init__(self):
		# call function to retrieve the api token
		self.token=APIAuthentication().get_token()
	
		# The link to the first page of the CIP API results
		self.interpretationlist = "https://cipapi.genomicsengland.nhs.uk/api/interpretationRequestsList/?format=json"
		# The link for the interpretationRequest
		self.interpretationrequest = "https://cipapi.genomicsengland.nhs.uk/api/interpretationRequests/%s/%s/?format=json"
	
		# moka statuses used to track samples
		self.awaiting_analysis_status = "1202218824" # default status when first added to db
		self.main = "1202218828" # status to denote main program
		self.pilot = "1202218827" # status to denote main program
		
		# variables to hold queries
		self.select_qry = ""
		self.insert_query = ""
		
		# dict to hold probands and IRs
		self.API_proband_list = {}
		
		# create and format a timestamp
		self.now = datetime.datetime.now()
		self.now = self.now.strftime('%Y-%m-%d %H:%M:%S')
		
		# variable to hold the existing proband list supplied as command line arg
		self.moka_list = ""
		
		# values to denote neg neg when inserting to moka
		self.neg_neg = "1"
		# value to denote non-neg neg cases
		self.non_neg_neg = "0"
				
	def get_command(self, argv):
		"""Capture a list of gel participant IDs already in moka that is passed as a argument"""
		# define expected inputs
		try:
			opts, args = getopt.getopt(argv, "c:")
		# raise errors 
		except getopt.GetoptError:
			sys.exit(2)

		# loop through the arguments
		for opt, arg in opts:
			if opt in ("-c"):
				# capture the list of probands from moka
				self.moka_list = str(arg)

	def build_url(self):	
		'''Capture the gel participant ID from the command line'''		
		# send the link to the first page of interpretation list to the function which returns the json for that page
		json = self.read_API_page(self.interpretationlist)
		# send the json to the parse json function which will create a list of proband IDs and interpretation request IDs
		self.parse_json(json)
		
	
	def read_API_page(self, url):
		'''This function takes a url and will return the response as a json'''
		# if proxy is set in the config file
		if proxy:
			response = requests.get(url, headers = {"Authorization": "JWT " + self.token}, proxies = proxy) # note space is required after JWT 
		# if no proxy
		else:
			response = requests.get(url, headers = {"Authorization": "JWT " + self.token}) # note space is required after JWT 
		
		# pass this in the json format to the parse_json function
		return response.json()
	
	
	def parse_json(self,json):
		"""
		This function takes a json object containing one page of the interpretation requests list.
		A list of probands already in moka is passed to this script as an argument, this is used to skip any samples that have already been imported
		This function populates a dictionary with a key of proband ID and a value is a list of tuples made up of interpretation requests, and the site name (used to distinguish between main and pilot program)		
		"""
		# loop through the results
		for sample in json['results']:
			# capture the proband id		
			proband = sample['proband']
			# check is the proband id is already in moka
			if proband in self.moka_list:
				pass
			else:
				# if the sample has a status we're interested in
				if sample["last_status"] in ["sent_to_gmcs", "report_generated", "report_sent"]:
					# check we are only looking at omicia interpretation requests
					if sample["cip"] == CIP:
						# check if proband is in dictionary
						if proband in self.API_proband_list:
							# if it is add the IR ID
							self.API_proband_list(proband).append((sample['interpretation_request_id'],sample['sites']))
						else:
							# else add to dict
							self.API_proband_list[proband] = [(sample['interpretation_request_id'],sample['sites'])]
					# if not Omicia CIP
					else:
						pass
				# if not a desired status
				else:
					pass

		# at end of the list read the next page
		if json['next']:
			# Call the function to read the API with url for next page
			json = self.read_API_page(json['next'])
			#send result to parse_json function
			self.parse_json(json)
		else:		
			# when all pages have been read call function to create insert statements
			self.parse_samples()
			
		
	def parse_samples(self):
		"""
		This function loops through the dictionary populated above and captures the highest interpretation request.
		It checks that there aren't two different unblocked interpretation requests (for the same cip)
		"""
		#loop through probands in dict
		for proband in self.API_proband_list:
			#set empty values for Interpretation requests 
			IR_id = 0
			max_version = 0
			
			# if multiple interpretation requests want the highest version number
			# for each tuple of (IR-ID,pilot/main status)
			for IR in self.API_proband_list[proband]:
				# the first element of the tuple is the interpretation_request eg 123-1
				interpretation_request=IR[0]
				# split the IR into interpretion request and the version
				interpretation_request_ID=int(interpretation_request.split('-')[0])
				version = int(interpretation_request.split('-')[1])
				
				# second element of the tuple is the site
				site=IR[1]
				
				# first time capture the IR_ID
				if IR_id == 0:
					IR_id = interpretation_request_ID
				# The IR_ID should be the same for each proband.
				elif IR_id != interpretation_request_ID:
					# if it doesn't match raise an error 
					raise ValueError('Different Interpretation request IDs for same proband (' + str(proband) + ', same CIP')

				# If this IR has a higher version
				if version > max_version:
					# capture the highest version
					max_version = version

				# look at the second element of the tuple to distinguish between main and pilot program cases
				if site == "GSTT":
					# set the program as the key for the pilot cases
					program = self.pilot
				else:
					# set the program as the key for the main cases
					program = self.main

			# call the function which reads the interpretation request
			negneg = self.identify_negative_negative(self.interpretationrequest % (IR_id, max_version))
			
			# print the folloing fields to be used in an insert query GEL_ProbandID,IR_ID,GEL_programme,Lab_Status,DateAdded, negneg
			print (str(proband), str(IR_id) + "-" + str(max_version), program, self.awaiting_analysis_status, self.now, negneg)

	def identify_negative_negative(self, url):
		"""
		This function identifies negative negative cases, returning 1 if is is a negative negative and 0 otherwise.
		
		The function receives the url for the interpretation request (IR).
		The IR is parsed to identify the highest cip version.
		Then tiered variants are assessed - if any tier 1 or tier2 variants are seen the case is not a negative negative.
		If any CIP candidate variants are present then the case is not a negative negative
		If the TieredVariants section is not present in the API json then 
		"""
		# pass the url to the function which reads the url. assign to interpretation_request
		interpretation_request = self.read_API_page(url)
		max_cip_ver = 0
		# loop through each report to find the highest cip_version
		for j in range(len(interpretation_request["interpreted_genome"])):
			# if the cip version is greater than that already seen
			if int(interpretation_request["interpreted_genome"][j]["cip_version"]) > max_cip_ver:
				# record the max cip version
				max_cip_ver=interpretation_request["interpreted_genome"][j]["cip_version"]
		
		# set variable to denote if neg_neg - initially say it is not a neg neg
		positive = False
		
		# check tiered variants exists (one or two cases don't have this in the API)
		if 'TieredVariants' not in interpretation_request['interpretation_request_data']['json_request']:
			# pass to avoid error when trying to loop through a non-existing list
			pass
		# see if there are any tiered variants (these are in a list)
		else:
			for variant in interpretation_request['interpretation_request_data']['json_request']['TieredVariants']:
				# there can be a list of classifications for a variant eg if the gene is in multiple panels. 
				# loop through these incase one panel has a different tier to the others
				for i in range(len(variant['reportEvents'])):
					# Look for tier1 and tier2 variants (ignore tier3)
					if variant['reportEvents'][i]['tier'] in ["TIER1","TIER2"]:
						# if present mark case as positive 
						positive = True
		
		# next look at CIP candidate variants, but no need to look if case already has tier1 or tier2 variant
		if not positive:
			# loop through the interpreted_genome, looking for the one with highest cip version
			for interpreted_genome in interpretation_request['interpreted_genome']:
				# if it's the highest cip version
				if interpreted_genome['cip_version'] == max_cip_ver:
					# check if there are any variants.
					if len(interpreted_genome['interpreted_genome_data']['reportedVariants']) > 0:
						#mark proband as positive
						positive = True
		
		# if any variants have been identified
		if positive:
			# return False to denote it's not a negative negative
			return self.non_neg_neg
		#otherwise 
		else:
			# return true to denote a negative negative case
			return self.neg_neg
					
if __name__ == "__main__":
	c = connect()
	c.get_command(sys.argv[1:])
	c.build_url()
