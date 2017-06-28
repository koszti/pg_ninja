#!/usr/bin/env python
import argparse
from pg_ninja import replica_engine
from pg_ninja import config_dir
configdir = config_dir()
configdir.set_config()

commands = [
	'create_schema',
	'init_replica',
	'start_replica',
	'upgrade_schema',
	'drop_schema', 
	'sync_obfuscation', 
	'add_table', 
	'add_source', 
	'drop_source', 
	'list_config', 
	'show_status' ,
	'stop_replica', 
	'disable_replica', 
	'enable_replica'
	]
command_help = 'Available commands, ' + ','.join(commands)


table_help =  'Specify the table\'s name to add to the existing replica. Multiple tables can be specified separated by comma. ' 
clean_help = 'Cleans the index definitions before the re-sync. Use with caution.'

parser = argparse.ArgumentParser(description='Command line for pg_ninja.',  add_help=True)
parser.add_argument('command', metavar='command', type=str, help=command_help)
parser.add_argument('--config', metavar='config', type=str,  default='default',  required=False)
parser.add_argument('--table', metavar='table', type=str,  default='*',  required=False, help=table_help)
parser.add_argument('--clean', default=False, required=False, help=clean_help, action='store_true')

args = parser.parse_args()

if args.command in commands:
	replica = replica_engine(args.config)
	if args.command == commands[0]:
		replica.create_service_schema()
	elif args.command == commands[1]:
		replica.init_replica()
	elif args.command == commands[2]:
		replica.run_replica()
	elif args.command == commands[3]:
		replica.upgrade_service_schema()
	elif args.command == commands[4]:
		replica.drop_service_schema()
	elif args.command == commands[5]:
		replica.sync_obfuscation(True, args.table, args.clean)
	elif args.command == commands[6]:
		replica.add_table(args.table)
	elif args.command == commands[7]:
		replica.add_source()
	elif args.command == commands[8]:
		replica.drop_source()
	elif args.command == commands[9]:
		replica.list_config()
	elif args.command == commands[10]:
		replica.show_status()
	elif args.command == commands[11]:
		replica.stop_replica()
	elif args.command == commands[12]:
		replica.stop_replica(allow_restart=False)
	elif args.command == commands[13]:
		replica.enable_replica()
		

else:
	print command_help
