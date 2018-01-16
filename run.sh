#!/bin/bash


if [ ! $(sudo docker ps -q -f name=pg) ];
then
    if [ $(sudo docker ps -aq -f status=exited -f name=pg) ];
    then
        echo "Starting Postgres.."
        sudo docker run --name pg -e POSTGRES_PASSWORD=postgres -d postgres
        echo "Complete"
    fi
fi

lein run "foo"
