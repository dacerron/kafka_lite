#!/bin/bash

echo "Please enter your csid"
read csid

login_username="${csid}@ugrad.cs.ubc.ca"
echo "Login using $login_username. Please enter your azure password"

az login -u $login_username

START_TIME=$SECONDS

echo "Please wait while VM is starting..."
# Start vm asynchronously
active_ids=$(az vm list -d --resource-group ServerResource --query "[?powerState=='VM deallocated' || powerState=='VM stopped'].id" -o tsv)
az vm start --no-wait --ids $active_ids
az vm wait --created --ids $active_ids --interval 15

ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo "Starting VMs took $(($ELAPSED_TIME/60)) min $(($ELAPSED_TIME%60)) sec"
