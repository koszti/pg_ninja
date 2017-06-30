pg_ninja
##############

.. image:: https://img.shields.io/github/issues/transferwise/pg_ninja.svg   
  :target: https://github.com/transferwise/pg_ninja/issues
	
.. image:: https://img.shields.io/github/forks/transferwise/pg_ninja.svg   
  :target: https://github.com/transferwise/pg_ninja/network

.. image:: https://img.shields.io/github/stars/transferwise/pg_ninja.svg   
  :target: https://github.com/transferwise/pg_ninja/stargazers
  
.. image:: https://img.shields.io/badge/license-Apache%202-blue.svg   
  :target: https://raw.githubusercontent.com/transferwise/pg_ninja/master/LICENSE
  
pg_ninja is a tool for replicating and obfuscating the data from MySQL to PostgreSQL compatible with Python 2.. 
The system use the library mysql-replication to pull the row images from MySQL which are transformed into a jsonb object. 
A pl/pgsql function decodes the jsonb and replays the changes into the PostgreSQL database.

The tool requires an initial replica setup which pulls the data from MySQL locked in read only mode. 
This is done by the tool running FLUSH TABLE WITH READ LOCK; .

pg_ninja can pull the data from a cascading replica when the MySQL slave is configured with log-slave-updates.

Current version: 0.1 DEVEL

Requirements
******************

Python: CPython 2.7 on Linux

MySQL: 5.5+

PostgreSQL: 9.5+ with the extension pg_crypto installed.

* `PyMySQL <https://pypi.python.org/pypi/PyMySQL>`_ 
* `argparse <https://pypi.python.org/pypi/argparse>`_
* `mysql-replication <https://pypi.python.org/pypi/mysql-replication>`_
* `psycopg2 <https://pypi.python.org/pypi/psycopg2>`_
* `PyYAML <https://pypi.python.org/pypi/PyYAML>`_
* `tabulate <https://pypi.python.org/pypi/tabulate>`_

Optionals for building documentation

* `sphinx <http://www.sphinx-doc.org/en/stable/>`_
* `sphinx-autobuild <https://github.com/GaretJax/sphinx-autobuild>`_


Setup 
**********

* clone the git repository
* Create a virtual environment and activate it
* Cd in the git repository main directory
* Install the package with pip install .
* Create a user on mysql for the replica (e.g. usr_replica)
* Grant access to usr on the replicated database (e.g. GRANT ALL ON sakila.* TO 'usr_replica';)
* Grant RELOAD privilege to the user (e.g. GRANT RELOAD ON \*.\* to 'usr_replica';)
* Grant REPLICATION CLIENT privilege to the user (e.g. GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica';)
* Grant REPLICATION SLAVE privilege to the user (e.g. GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica';)
* Create the replica user on PostgreSQL 
* Create a database on PostgreSQL with owner the replica user created at the step before
* Create the pg_crypto extension on the replica database
* Run ninja.py to create the configuration directory ~/.pg_ninja
* cd in  ~/.pg_ninja and copy the configuration-example.yaml to another file (e.g. default.yaml)
* Copy the obfuscation-example.yaml file to another file (e.g. obfuscation-example.yaml)
* Setup the connection in default.yaml
* Setup the obfuscation stragegies in obfuscation-example.yaml
* Create the service schema with ninja.py create_schema --config default
* Add the replica source with ninja.py add_source --config default
* Init the replica  ninja.py init_replica--config default . Warning this will lock the mysql db in read only mode for the time required by the replica init.
* Start  the replica with ninja.py start_replica --config default

Documentation
*****************************
In order to build the documentation, within the the virtual environment you will need sphinx.

Activate the virtual environment then run

.. code-block:: none

	pip install sphinx


cd in the documentation directory and run 

.. code-block:: none

	cd docs
	make html
	
Sphinx will build the documents in the subdirectory _build/html .


Configuration parameters
********************************
The system wide install is now supported correctly. 

The first time ninja.py is executed creates a configuration directory in $HOME/.pg_ninja.
Inside the directory there are two subdirectories. 

* config is where the configuration files live. Use config-example.yam and obfuscation-example.yamll as templates. 
For configuring properly the logs and pid you should either use an absolute path or provide the home alias. 
The config-example.yaml provides the configuration for the home directory setup.

* pid is where the replica pid file is created. it can be changed in the configuration file

* logs is where the replica logs are saved if log_dest is file. It can be changed in the configuration file

The file config-example.yaml is stored in **~/.pg_ninja/config** and should be used as template for the other configuration files. 
The file obfuscation-example.yaml is stored in **~/.pg_ninja/config** and should be used as template for the other obfuscation files. 


**do not use config-example.yaml or obfuscation-example.yaml** directly. 
The files are overwritten when pg_ninja is upgraded.

Is it possible to have multiple configuration files for configuring the replica from multiple source databases. 
It's compulsory to chose different destination schemas on postgresql and to have an unique source_name.

Each source requires to be started in a separate process (e.g. a cron entry).

Configuration file 
********************************
The configuration file is a yaml file. Each parameter controls the
way the program acts.

* my_server_id the server id used by the mysql replica. 
* copy_max_memory the slice's size in rows when copying a table from MySQL to PostgreSQL during the init_replica
* my_database mysql database to replicate. a schema with the same name will be initialised in the postgres database
* pg_database destination database in PostgreSQL. 
* copy_mode the allowed values are 'file'  and 'direct'. With 'file' the table is first dumped in a csv file then loaded in PostgreSQL. With 'direct' the copy happens in memory. 
* hexify lists the data types that require coversion in hex (e.g. blob, binary). The conversion happens on the initial copy and during the replica.
* log_dir directory where the logs are stored
* log_level logging verbosity. allowed values are debug, info, warning, error
* log_dest log destination. stdout for debugging purposes, file for the normal activity.
* my_charset mysql charset for the copy (please note the replica is always in utf8)
* pg_charset PostgreSQL connection's charset. 
* tables_limit yaml list with the tables to replicate. if empty the entire mysql database is replicated.
* exclude_tables list with the tables to exclude from the initial and copy and replica.
* email_config email settings for sending the email alerts (e.g. when the replica starts)
* obfuscation_file path to the obfuscation file 
* schema_clear the schema with the full replica with data in clear
* schema_obf the schema with the tables with the obfuscated fields listed in the obfuscation file. the tables not listed are exposed as views selecting from the schema in clear.
* replica_batch_size: 1000
* reply_batch_size: 1000
* source_name: 'default'
* sleep_loop: 5
* batch_retention: '7 days'
* obfuscation_file
* skip_view skip view drop and creation in obfuscated schema
* out_dir: '/tmp/'
* log_days_keep: 10


    

MySQL connection parameters
    
.. code-block:: yaml

    mysql_conn:
        host: localhost
        port: 3306
        user: replication_username
        passwd: never_commit_passwords


PostgreSQL connection parameters

.. code-block:: yaml

    pg_conn:
        host: localhost
        port: 5432
        user: replication_username
        password: never_commit_passwords

	
pg_ninja can send emails under certain circumstances (end of init_replica, end of sync_obfuscation).
The smtp configuration is done in the email_config parameter. It's also possible to use tls or 
authenticate against the smtp server.
  
 
.. code-block:: yaml

        email_config: 
                subj_prefix: 'PGNINJA'
                smtp_login: Yes
                smtp_server: 'smtp.foo.bar'
                smtp_port: 587
                email_from: pgobfuscator@foo.bar
                smtp_username: login@foo.bar
                smtp_password: never_commit_passwords
                smtp_tls: Yes
                email_rcpt:
                       - alert@foo.bar


Obfuscation
**********************
The obfuscation file is a simple yaml file where the table and the fields requiring obfuscation are listed.

There are 
the mode normal can hash the entire field or keep an arbitrary number of characters not obfuscated (useful for 
running joins).

The obfuscation strategies are explained below.

.. code-block:: yaml


	---
	# obfuscate the entire field text_full in table example_01 using SHA256 
	example_01:
	    text_full:
		mode: normal
		nonhash_start: 0
		nonhash_length: 0

	# obfuscate the field text_partial in table example_02 using SHA256 preserving the first two characters        
	example_02:
	    text_partial:
		mode: normal
		nonhash_start: 1
		nonhash_length: 2

		
	# obfuscate the field date_field in table example_03 changing the date to the first of january of the given year
	# e.g. 2015-05-20 -> 2015-01-01
	example_03:
	    date_field:
		mode: date
	    
	# obfuscate the field numeric_field (integer, double etc.) in table example_04 to 0
	example_04:
	    numeric_field:
		mode: numeric

	# obfuscate the field nullable_field changing the value to NULL
	example_05:
		nullable_field:
			mode: setnull
		
Usage
**********************
The script ninja.py have a basic command line interface.



* drop_schema Drops the schema sch_chameleon with cascade option
* create_schema Create the schema sch_chameleon
* upgrade_schema Upgrade an existing schema sch_chameleon
* init_replica Creates the table structure and copy the data from mysql locking the tables in read only mode. It saves the master status in sch_chameleon.t_replica_batch.
* start_replica Starts the replication from mysql to PostgreSQL using the master data stored in sch_chameleon.t_replica_batch and update the master position every time an new batch is processed.
* sync_obfuscation synchronise the obfuscated tables with the schema in clear
* add_table add a table to a running replica
* add_source add a replica source to the replica catalogue
* drop_source remove a source from the replica catalogue
* list_config list the available configurations
* show_status show the status of all registered sources with the replica lag
* stop_replica stops a running replica
* disable_replica stops  a running replica and prevents the replica to start again
* enable_replica enables the replica start



Example
**********************

In MySQL create a user for the replica.

.. code-block:: sql

    CREATE USER usr_replica ;
    SET PASSWORD FOR usr_replica=PASSWORD('replica');
    GRANT ALL ON sakila.* TO 'usr_replica';
    GRANT RELOAD ON *.* to 'usr_replica';
    GRANT REPLICATION CLIENT ON *.* to 'usr_replica';
    GRANT REPLICATION SLAVE ON *.* to 'usr_replica';
    FLUSH PRIVILEGES;
    
Add the configuration for the replica to my.cnf (requires mysql restart)

.. code-block:: ini
    
    binlog_format= ROW
    log-bin = mysql-bin
    server-id = 1

In PostgreSQL create a user for the replica and a database owned by the user

.. code-block:: sql

    CREATE USER usr_replica WITH PASSWORD 'replica';
    CREATE DATABASE db_replica WITH OWNER usr_replica;

Check you can connect to both databases from the replication system.

For MySQL

.. code-block:: bash

    mysql -p -h hostreplica -u usr_replica sakila 
    Enter password: 
    Reading table information for completion of table and column names
    You can turn off this feature to get a quicker startup with -A

    Welcome to the MySQL monitor.  Commands end with ; or \g.
    Your MySQL connection id is 116
    Server version: 5.6.30-log Source distribution

    Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

    Oracle is a registered trademark of Oracle Corporation and/or its
    affiliates. Other names may be trademarks of their respective
    owners.

    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

    mysql> 
    
For PostgreSQL

.. code-block:: bash

    psql  -h hostreplica -U usr_replica db_replica
    Password for user usr_replica: 
    psql (9.5.4)
    Type "help" for help.
    db_replica=> 

Setup the connection parameters in default.yaml

.. code-block:: yaml

    ---
    #global settings
    my_server_id: 100
    replica_batch_size: 10000
    my_database:  sakila
    pg_database: db_replica

    #mysql connection's charset. 
    my_charset: 'utf8'
    pg_charset: 'utf8'

    #include tables only
    tables_limit:

    #mysql slave setup
    mysql_conn:
        host: my_test
        port: 3306
        user: usr_replica
        passwd: replica

    #postgres connection
    pg_conn:
        host: pg_test
        port: 5432
        user: usr_replica
        password: replica
    


Initialise the schema and the replica with


.. code-block:: bash
    
    ninja.py create_schema --config default
    ninja.py add_source --config default
    ninja.py init_replica --config default


Start the replica with

.. code-block:: bash

    ninja.py start_replica  --config default
	



