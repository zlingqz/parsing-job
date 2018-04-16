#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import matplotlib
matplotlib.use('Agg')


import pandas as pd
import pymysql
import matplotlib.pyplot as plt


from matplotlib.font_manager import FontProperties


def group_and_sort(dafr=None, order=''):
	grouped = dafr.groupby(order).sum(axis=1)
	sorted = grouped.sort_values(by='num', ascending=False)
	return sorted#.iloc[: , 2]


def autolabel(rects):
	for rect in rects:
		height = rect.get_height()
		plt.text(rect.get_x() + rect.get_width() / 2., 1.03 * height, "%s" % float(height))


def main():
	db = pymysql.connect("localhost", "root", "1q2w3e!Q@W#E", "PARSING_JOBS", charset='utf8')
	df = pd.read_sql("SELECT * from job_statistics", db)
	print('\n\n************    city_sum_sorted    ************')
	city_sum_sorted = group_and_sort(dafr=df, order='job_location')
	print(city_sum_sorted.shape)
	# print(city_sum_sorted.head(20))
	# print(type(city_sum_sorted.head(20)))
	zhfont1 = FontProperties(fname='/usr/share/fonts/simhei.ttf')
	# zhfont1 = FontProperties(fname='C:\Windows\Fonts\simkai.ttf')
	plt.figure(1)
	fig, ax = plt.subplots(figsize=(6,5))
	fig.subplots_adjust(bottom=0.15, left=0.2)
	ax.bar(city_sum_sorted.head(20).index.values.tolist(), city_sum_sorted.head(20).values[:, 2].tolist(), width=0.5, align="center", alpha=0.4, color='b')
	# plt.xlabel('城市', fontproperties=zhfont1, fontweight='bold', horizontalalignment='right')
	ax.set_ylabel('招聘数', fontproperties=zhfont1)
	plt.xticks(city_sum_sorted.head(20).index.values.tolist(), city_sum_sorted.head(20).index.values.tolist(), fontproperties=zhfont1, rotation=60)
	# ax.xaxis.set_major_formatter(zhfont1)
#	ax.tick_params(axis='x', rotation=60, font=zhfont1)
	# autolabel(rect)
	# plt.show()
	plt.savefig('city_sum_sorted.pdf')
	# print(city_sum_sorted.head(20).index.values.tolist())
	# print(type(city_sum_sorted.head(20).index.values.tolist()))
	# print(city_sum_sorted.head(20).values[:, 2].tolist())
	# print(type(city_sum_sorted.head(20).values[:, 2].tolist()))
	# print(city_sum_sorted.columns.values)

	print('\n\n************    job_sum_sorted    ************')
	job_sum_sorted = group_and_sort(dafr=df, order='job')
	plt.figure(2)
	fig, ax = plt.subplots(figsize=(6,5))
	fig.subplots_adjust(bottom=0.3, left=0.2)
	job_sum_sorted_head_index = job_sum_sorted.head(21).index.values.tolist()
	job_sum_sorted_head_values = job_sum_sorted.head(21).values[:, 2].tolist()
	for a in list(range(21)):
		if job_sum_sorted_head_index[a] == '其他':
			del job_sum_sorted_head_index[a]
			del job_sum_sorted_head_values[a]
			break
	ax.bar(job_sum_sorted_head_index, job_sum_sorted_head_values, width=0.5, align="center", alpha=0.4, color='b')
	ax.set_ylabel('招聘数', fontproperties=zhfont1)
	plt.xticks(job_sum_sorted_head_index, job_sum_sorted_head_index, fontproperties=zhfont1, rotation=60)
	ax.set_xticklabels(job_sum_sorted_head_index, fontdict={'horizontalalignment': 'right'})
	plt.savefig('job_sum_sorted.pdf')
	
	plt.figure(3)
	fig, ax = plt.subplots(figsize=(6,5))
	fig.subplots_adjust(bottom=0.35, left=0.2)
	job_field_sum_sorted = group_and_sort(dafr=df, order='job_field')
	ax.bar(job_field_sum_sorted.head(20).index.values.tolist(), job_field_sum_sorted.head(20).values[:, 2].tolist(), width=0.5, align="center", alpha=0.4, color='b')
	ax.set_ylabel('招聘数', fontproperties=zhfont1)
	plt.xticks(job_field_sum_sorted.head(20).index.values.tolist(), job_field_sum_sorted.head(20).index.values.tolist(), fontproperties=zhfont1, rotation=60)
	ax.set_xticklabels(job_field_sum_sorted.head(20).index.values.tolist(), fontdict={'horizontalalignment': 'right'})
	plt.savefig('job_field_sum_sorted.pdf')


if __name__ == '__main__':
	main()
