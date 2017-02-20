#!/bin/bash
ACTION=$1
HERE=`dirname $0`
EXITFILE="pid/exit_process.trg"
cd ${HERE}
if [ "${ACTION}" == "stop" ]
then
	touch ${EXITFILE}
elif [ "${ACTION}" == "start" ]
then
	rm -f ${EXITFILE}
fi
