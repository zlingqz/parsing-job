#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from atexit import register
from random import uniform
from time import sleep, ctime, time
from concurrent.futures import ThreadPoolExecutor, as_completed


from urllib.request import urlopen
from urllib.parse import unquote
import re
import pymysql
from bs4 import BeautifulSoup
from threading import Lock


attr_error = []
print('connect to MySQL db')
db = pymysql.connect("localhost", "root", "1q2w3e!Q@W#E", "PARSING_JOBS", charset='utf8')
cursor = db.cursor()
# cursor.execute("CREATE TABLE job_statistics(job_location VARCHAR(100), job_field VARCHAR(100), job VARCHAR(100), num INT)")

start_time = time()
Urls = []
f = open('urls.txt', 'r')
for line in f.readlines():
	if line.strip():
		Urls.append(line.strip())
f.close()
lock = Lock()


class TestThreadPoolExecutor(object):
	def __init__(self):
		self.nt = 1
		self.time_out = 0
		self.thread_pool = ThreadPoolExecutor(max_workers=10)
		self.futures_url = dict()
		self.type_error = []
	

	def parsing_categary(self, url=None, nt=0):
		'parsing the url and get the job statistics'
		city_quote = re.search('jl=([\w|%|/|（|）]+)&sm=0&p=1', url).group(1)
		city_unquote = unquote(city_quote)
		try:
			cat_page = urlopen(url, timeout=7)
			cat_bs4 = BeautifulSoup(cat_page, 'lxml')
			categary = cat_bs4.find('div', attrs={'class': 'search_newlist_topmain2 fl', 'id': 'search_jobtype_tag'})
			n = 0
			key1 = ''
			try:
				print('find job num    ', str(ctime()), '    ', str(nt), '    ', url)
				for ty in categary.find_all('a'):
					if n:
						m = re.match('(.*?)\((\d+)\)', ty.text)
						if n == 1:
							key1 = m.group(1)
						else:
							lock.acquire()
							try:
								se = cursor.execute("SELECT * from job_statistics WHERE (job_location = '%s' and job_field = '%s' and job = '%s')" % (city_unquote, key1, m.group(1)))
								if se:
									cursor.execute("UPDATE job_statistics SET num = '%d' WHERE (job_location = '%s' and job_field = '%s' and job = '%s')" % (int(m.group(2)), city_unquote, key1, m.group(1)))
								else:
									cursor.execute("INSERT INTO job_statistics(job_location, job_field, job, num) VALUES ('%s', '%s', '%s', '%d')" % (city_unquote, key1, m.group(1), int(m.group(2))))
								db.commit()
							except pymysql.err.InterfaceError:
								print('pymysql.err.InterfaceError.__context__:', pymysql.err.InterfaceError.__context__, city_unquote, key1, m.group(1), m.group(2), '    ', str(nt))
							lock.release()
					n += 1
			except AttributeError:
				attr_error.append(url)
			sleep(uniform(1, 3))
		except TimeoutError:
			Urls.append(url)
			self.time_out += 1


	def completed(self):
		for future in as_completed(self.futures_url):
			url = self.futures_url[future]
			try:
				future.result()
			except TypeError:
				self.type_error.append(url)
			except Exception as e:
				print('Run thread url ('+url+') error. '+str(e))
				Urls.append(url)
			del self.futures_url[future]
		sleep(60)
		if Urls:
			self.runner()
	

	def runner(self):
		while Urls:
			url = Urls.pop()
			future = self.thread_pool.submit(self.parsing_categary, url, self.nt)
			self.futures_url[future] = url
			self.nt += 1
		sleep(30)
		self.completed()
		print('time out urls:', str(self.time_out))
		if self.type_error:
			print('type_error:')
			print(self.type_error)


@register
def _atexit():
	end_time = time()
	print('all done at:', ctime())
	hour = int((end_time - start_time)//360)
	minite = int((end_time - start_time) % 360//60)
	sec = int((end_time - start_time) % 360 % 60)
	sp = 'spending ' + str(hour) + 'h' + str(minite) + 'min' + str(sec) + 's'
	print(sp)
	se = cursor.execute("SELECT * from job_statistics")
	print('There are {} items'.format(str(se)))
	db.close()


if __name__ == '__main__':
	print('start TestThreadPoolExecutor', ctime())
	TestThreadPoolExecutor().runner()
