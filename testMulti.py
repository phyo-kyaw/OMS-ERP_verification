from multiprocessing import Process
import os
import time

def waitSleep():
	time.sleep(3)
	
def info(title):
	print 'info started'
	print(title)
	print('module name:', __name__)
	if hasattr(os, 'getppid'):  # only available on Unix
		print('parent process:', os.getppid())
	print('process id:', os.getpid())
	waitSleep()
	

def f(name):
	print 'f started'
	#info('function f')
	print('hello', name)
	waitSleep()
		
	
	

if __name__ == '__main__':
    info('main line')
    p = Process(target=f, args=('bob',))
    p.start()
    p.join()