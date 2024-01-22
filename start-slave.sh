#!/bin/bash

# the default node number is 3
N=${1:-1}

# start hadoop slave container

sudo docker rm -f hadoop-slave$N &> /dev/null
echo "start hadoop-slave$N container..."
sudo docker run -itd \
				-p 50010:50010 \
				-p 50020:50020 \
				-p 50060:50060 \
				--name hadoop-slave$N\
				--hostname hadoop-slave$N \
				kiwenlau/hadoop:1.0 &> /dev/null

