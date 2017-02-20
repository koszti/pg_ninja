#!/usr/bin/env python
import argparse
from pg_chameleon import replica_engine
commands = [
					'create_schema',
					'init_replica',
					'start_replica',
					'upgrade_schema',
					'drop_schema', 
					'take_snapshots', 
					'sync_obfuscation', 
					'sync_replica'
	]
command_help = 'Available commands, ' + ','.join(commands)

parser = argparse.ArgumentParser(description='Command line for pg_chameleon.')
parser.add_argument('command', metavar='command', type=str, help=command_help)
parser.add_argument('--snapshot', metavar='snapshot', type=str,  default='all',  required=False)
args = parser.parse_args()

if args.command in commands:
	replica = replica_engine(args.command)
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
		replica.take_snapshot(args.snapshot)
	elif args.command == commands[6]:
		replica.sync_obfuscation()
	elif args.command == commands[7]:
		replica.sync_replica()

else:
	print command_help
