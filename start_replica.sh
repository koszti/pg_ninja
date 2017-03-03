#!/bin/bash
ENV=env
HERE=`dirname $0`
cd ${HERE}
. ${ENV}/bin/activate
./ninja.py start_replica
