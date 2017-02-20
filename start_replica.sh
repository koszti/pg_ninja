#!/bin/bash
ENV=env
HERE=`dirname $0`
cd ${HERE}
. ${ENV}/bin/activate
./pg_chameleon.py start_replica
