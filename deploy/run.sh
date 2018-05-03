#!/bin/bash

# This script assumes that VM has been started, otherwise, run sh start_vms.sh

# constants
PORT_NUM=12345

SERVER_PUBLIC_IP=52.151.46.93
SERVER_IP_PORT="10.0.0.4:12345"
SERVER_PRIVATE_IP=10.0.0.4

BROKER1_PUBLIC_IP=52.183.125.6
BROKER2_PUBLIC_IP=40.125.65.98
BROKER3_PUBLIC_IP=52.229.57.228
BROKER4_PUBLIC_IP=52.183.114.240
BROKER5_PUBLIC_IP=52.151.62.1

PRODUCER1_PUBLIC_IP=52.151.63.241
PRODUCER2_PUBLIC_IP=52.229.59.83
PRODUCER3_PUBLIC_IP=40.125.64.106

CONSUMER1_PUBLIC_IP=52.175.246.43
CONSUMER2_PUBLIC_IP=52.175.237.141

proj_path="~/go/src/stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/"

echo "Please enter the program that you want to run (s - Server, b1 - Broker1, p - Producer, c - Consumer)"
read name

case $name in
  s) echo "\nStarting Server\n";
    ssh -t "j9x9a@$SERVER_PUBLIC_IP" "source .profile;cd $proj_path; go run server/server.go $PORT_NUM; bash";;
  b1) echo "\nStarting Broker 1\n";
    ssh -t "j9x9a@$BROKER1_PUBLIC_IP" "source .profile; cd $proj_path; go run broker/broker.go $SERVER_IP_PORT broker/config.json 1; bash";;
  b2) echo "\nStarting Broker 2\n";
    ssh -t "j9x9a@$BROKER2_PUBLIC_IP" "source .profile; cd $proj_path; go run broker/broker.go $SERVER_IP_PORT broker/config.json 2; bash";;
  b3) echo "\nStarting Broker 3\n";
    ssh -t "j9x9a@$BROKER3_PUBLIC_IP" "source .profile; cd $proj_path; go run broker/broker.go $SERVER_IP_PORT broker/config.json 3; bash";;
  b4) echo "\nStarting Broker 4\n";
    ssh -t "j9x9a@$BROKER4_PUBLIC_IP" "source .profile; cd $proj_path; go run broker/broker.go $SERVER_IP_PORT broker/config.json 4; bash";;
  b5) echo "\nStarting Broker 5\n";
    ssh -t "j9x9a@$BROKER5_PUBLIC_IP" "source .profile; cd $proj_path; go run broker/broker.go $SERVER_IP_PORT broker/config.json 5; bash";;
  p1) echo "\nStarting Producer 1\n";
    ssh -t "j9x9a@$PRODUCER1_PUBLIC_IP" "source .profile; cd $proj_path; go run app/producer_client.go $SERVER_IP_PORT; bash";;
  p2) echo "\nStarting Producer 2\n";
    ssh -t "j9x9a@$PRODUCER1_PUBLIC_IP" "source .profile; cd $proj_path; go run app/producer_client2.go $SERVER_IP_PORT; bash";;
  p3) echo "\nStarting Producer 3\n";
    ssh -t "j9x9a@$PRODUCER1_PUBLIC_IP" "source .profile; cd $proj_path; go run app/producer_client3.go $SERVER_IP_PORT; bash";;
  pdog1) echo "\nStarting Producer 1\n";
    ssh -t "j9x9a@$PRODUCER2_PUBLIC_IP" "source .profile; cd $proj_path; go run app/producer_doggo.go $SERVER_IP_PORT :8080; bash";;
  pdog2) echo "\nStarting Producer 1\n";
    ssh -t "j9x9a@$PRODUCER3_PUBLIC_IP" "source .profile; cd $proj_path; go run app/producer_doggo.go $SERVER_IP_PORT :8080; bash";;
  c1) echo "\nStarting Consumer 1\n";
    ssh -t "j9x9a@$CONSUMER1_PUBLIC_IP" "source .profile; cd $proj_path; go run app/consumer_client.go $SERVER_IP_PORT; bash";;
  c2) echo "\nStarting Consumer Doggo\n";
    ssh -t "j9x9a@$CONSUMER2_PUBLIC_IP" "source .profile; cd $proj_path; go run app/consumer_doggo.go $SERVER_IP_PORT :8080; bash";;
  *) echo "\nCommand not found\n"
esac
