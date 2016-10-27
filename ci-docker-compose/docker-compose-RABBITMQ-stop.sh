#!/bin/bash

LOCAL_HOST="${LOCAL_HOST:-localhost}"
RETRIES="${RETRIES:-70}"

PORT_TO_CHECK=5672

WAIT_TIME="${WAIT_TIME:-5}"

function netcat_port() {
    local PASSED_HOST="${2:-$LOCAL_HOST}"
    local RABBITMQ_STOPPED=1
    local counter=1
    for i in $( seq 1 "${RETRIES}" ); do
        ((counter++))
        if [ "${counter}" -gt 2 ]
        then
            echo "Rabbitmq is still running. Will try to stop again in [${WAIT_TIME}] seconds"
        fi
        sleep "${WAIT_TIME}"
        nc -v -w 1 ${PASSED_HOST} $1 && continue
        echo "Rabbitmq stopped..."
        RABBITMQ_STOPPED=0
        break
    done
    return ${RABBITMQ_STOPPED}
}

export -f netcat_port

dockerComposeFile="docker-compose-RABBITMQ.yml"
docker-compose -f $dockerComposeFile kill

RABBITMQ_STOPPED="no"

echo "Waiting for RabbitMQ to stop for [$(( WAIT_TIME * RETRIES ))] seconds"
netcat_port $PORT_TO_CHECK && RABBITMQ_STOPPED="yes"

if [[ "${RABBITMQ_STOPPED}" == "no" ]] ; then
    echo "RabbitMQ failed to stop..."
    exit 1
fi