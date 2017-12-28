'''
Created on 7 Jun 2017
This script is designed to be called by MOKA and run from a trust machine (using the python executable on the S drive).
The script recieves a command to execute on the server
It then ssh's into the server using the details in the server_details.py script
A tuple containing stdout and stderr is returned.
@author: ajones7
'''
import getopt
import paramiko
import sys
import ServerDetails as server_details
import MokaConnection as moka_connection
import pyodbc

class ssh_n_run():
	def __init__(self):
		# set the command to run on the server
		self.command = ""
		
		# variables to hold the standard out and standard error from the command executed on the server
		self.standard_out = []
		self.standard_error = []

		#list of all probands in moka
		self.probands_in_moka = []
		
		# using pyodbc specify database connection details
		self.cnxn = pyodbc.connect(moka_connection.dev_database_connection) # use the mokadata database connection details specified in the connection file
		# create a cursor to connect to database 
		self.cursor = self.cnxn.cursor()
		
		# variables for insert and select queries
		self.select_qry = ""
		self.insert_query = ""

	def import_to_moka(self):
		"""
		This function executes the command on the server and parses the output.
		If there are any GEL proband IDs which are not in moka they are imported. 
		"""
		# pull out all probands in moka
		self.pull_out_moka()

		# set the command to execute on the server
		self.command = "/home/mokaguys/miniconda2/envs/pyODBC/bin/python /home/mokaguys/Documents/CIP_API_development/return_all_API_cases.py -c \"" + str(self.probands_in_moka) + "\""
		# execute command on server
		self.execute_command()
		
		count = 0 
		# loop through the standard out from the execute command function
		for proband in self.standard_out:
			# use the standard out to create a insert query
			self.insert_query =  "insert into GEL100KAnalysisStatus (GELProbandID,IRID,GELProgram,LabStatus,DateAdded,NegNeg) values " + i.rstrip()
			# execute the inster query
			self.insert_query_function()
			count += 1
			#print count
		print "imported " + str(count) + "  cases"
		

	def pull_out_moka(self):
		''' read all probands already in moka'''	
		# get all gel_probands in moka
		self.select_qry="select GELProbandID from dbo.[GEL100KAnalysisStatus]"
		
		# a tuple is returned for each value in the select query
		# loop through and extract the proband ID (first element in the tuple)
		for sample in self.select_query():
			self.probands_in_moka.append(sample[0])


	def execute_command(self):
		"""
		This function SSH's into the server and executes the command, printing the response
		The paramiko ssh package is used to connect to the server using the user details specified in the server_details.py script
		The standard out and standard error is captured and printed
		"""	
		# ssh client
		ssh = paramiko.SSHClient()
		# auto accept host key without prompting and requiring response from a user
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		# connect to server using details imported from from server_details.py
		ssh.connect(server_details.hostname, username = server_details.username, password = server_details.password)
		# send command
		stdin, stdout, stderr = ssh.exec_command(self.command)
		# return a tuple containing stdout and stderr, with newline characters removed.
		self.standard_out = stdout.readlines() 
		self.standard_error = stderr.readlines()
		# close connection
		ssh.close()


	def insert_query_function(self):
		'''This function executes an insert query'''
		# execute the insert query
		self.cursor.execute(self.insert_query)
		self.cursor.commit()


	def select_query(self):
		'''This function is called to retrieve the whole result of a select query '''
		# Perform query and fetch all
		result = self.cursor.execute(self.select_qry).fetchall()

		# return result
		if result:
			return(result)
		else:
			# if no result return an empty list
			result=[]
			return(result)
		

if __name__ == '__main__':
	go = ssh_n_run()
	go.import_to_moka()
	
