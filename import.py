# -*- coding: utf-8 -*-

import sys

import tracdash


def main():
	tracdash.init_logging(console=False, file=True)

	# ElasticSearch ip:port addresses
	es_ips = ['127.0.0.1']
	
	# file containing list of jsonl.gz files to import
	targ_file = sys.argv[1]
	files = []
	
	# ElasticSearch index name to update
	index_name = sys.argv[2]

	with open(targ_file) as f:
		for line in f:
			line = line.strip()
			if line:
				files.append(line.strip())
	
	tracdash.import_files(files, es_ips, index_name)



if __name__ == "__main__":
	main()
