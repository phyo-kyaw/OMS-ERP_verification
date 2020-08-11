from glob import glob
import fileinput
import os

tt = 'nsSO'
file_list = glob(tt + "*")
with open('result.csv', 'w') as file:
	input_lines = fileinput.input(file_list)
	file.writelines(input_lines)