#!/usr/bin/env python
# -*- coding: utf-8 -*-
from os import listdir
from os.path import  isfile, join
from setuptools import setup
from distutils.sysconfig import get_python_lib

python_lib=get_python_lib()

package_data = ('%s/pg_ninja' % python_lib, ['LICENSE'])

	

sql_up_path = 'sql/upgrade'
conf_dir = "/%s/pg_ninja/config" % python_lib
sql_dir = "/%s/pg_ninja/sql" % python_lib
sql_up_dir = "/%s/pg_ninja/%s" % (python_lib, sql_up_path)


data_files = []
conf_files = (
		conf_dir, 
		[
			'config/config-example.yaml', 
			'config/obfuscation-example.yaml', 
			'config/snapshots-example.yaml'
		]
	)

sql_src = ['sql/create_schema.sql', 'sql/drop_schema.sql']

sql_upgrade = ["%s/%s" % (sql_up_path, file) for file in listdir(sql_up_path) if isfile(join(sql_up_path, file))]

sql_files = (sql_dir,sql_src)
sql_files = (sql_dir,sql_src)
sql_up_files = (sql_up_dir,sql_upgrade)


data_files.append(conf_files)
data_files.append(sql_files)
data_files.append(sql_up_files)



setup(
	name="pg_ninja",
	version="v1.0-alpha1",
	description="MySQL to PostgreSQL replica and obfuscation",
	long_description="""pg_ninja is a tool for replicating and obfuscating the data in real time from MySQL to PostgreSQL, compatible with Python 2.7.
The system use the library mysql-replication to pull the row images from MySQL which are transformed into a jsonb object. 
A pl/pgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool requires an  initial replica setup which pulls the data from MySQL in read only mode. 
This is done by the tool running FLUSH TABLE WITH READ LOCK;  .

pg_ninja can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.

""",
	author="Federico Campoli",
	author_email="federico.campoli@transferwise.com",
	url="https://www.transferwise.com/",
	platforms=[
		"linux"
	],
	classifiers=[
		
		"Environment :: Console",
		"Intended Audience :: Developers",
		"Intended Audience :: Information Technology",
		"Intended Audience :: System Administrators",
		"Natural Language :: English",
		"Operating System :: POSIX :: BSD",
		"Operating System :: POSIX :: Linux",
		"Programming Language :: Python",
		"Programming Language :: Python :: 2.7",
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: 3.3",
		"Programming Language :: Python :: 3.4",
		"Programming Language :: Python :: 3.5",
		"Programming Language :: Python :: 3.6",
		"Topic :: Database :: Database Engines/Servers",
		"Topic :: Other/Nonlisted Topic"
	],
	py_modules=[
		"pg_ninja.__init__",
		"pg_ninja.lib.global_lib",
		"pg_ninja.lib.mysql_lib",
		"pg_ninja.lib.pg_lib",
		"pg_ninja.lib.sql_util", 
		"pg_ninja.lib.email_lib"
	],
	scripts=[
		"scripts/ninja.py"
	],
	install_requires=[
		'PyMySQL>=0.7.6', 
		'argparse>=1.2.1', 
		'mysql-replication>=0.11', 
		'psycopg2>=2.7.0', 
		'PyYAML>=3.11', 
		'tabulate>=0.7.7', 
					
	],
	data_files = data_files, 
	include_package_data = True
	
)
