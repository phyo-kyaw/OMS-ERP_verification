import csv
from glob import glob
import fileinput
import os

class FileOperation:

	def _init__(self, fileOMS, fileNS):
		pass
		
	def GetDifference(self, fileOMS, fileNS, fileDiff):
	
		
		file1 = open(fileOMS, 'rb')
		reader = csv.reader(file1)
		rows_list1 = []
		rows_list = []
		rows_dict = {}
		for row in reader:
			  rows_list1.append(row[0])
			  rows_list.append(row[2])
		file1.close()   # <---IMPORTANT
		
		file_list = glob( "*x" + fileNS)
		with open(fileNS, 'w') as file:
			input_lines = fileinput.input(file_list)
			file.writelines(input_lines)

		file2 = open(fileNS, 'rb')
		reader = csv.reader(file2)
		rows_list2 = []
		for row in reader:
			  rows_list2.append(row[0])
		file2.close()   # <---IMPORTANT

		#print len(rows_list1)
		#print len(rows_list2)

		setOMS = set(rows_list1)
		setNS = set(rows_list2)

		missedSet = setOMS.difference(setNS)
		missedList = list(missedSet)
		#print missedList
		file3 = open(fileDiff, 'wb')
		c = csv.writer(file3)
		rows_dict = dict(zip(rows_list1, rows_list))
		#print rows_dict
		for row in missedList:
			c.writerow((row, rows_dict[row]))
			#print rows_dict[row]
		file2.close()
		
		print fileDiff, " --> ", len(missedList)
		
		for filename in glob('*x' + fileNS):
			os.remove(filename)