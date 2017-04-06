#!/bin/bash
ENV=env
HERE=`dirname $0`
SNAPNAME=$1
cd ${HERE}
. ${ENV}/bin/activate
if [ "${SNAPNAME}" != "" ]
then
        SNAPOPT="--snapshot ${SNAPNAME}"
fi
./ninja.py take_snapshots ${SNAPOPT}
