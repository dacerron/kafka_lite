#!/bin/bash

echo "Please enter your csid"
read csid

login_username="${csid}@ugrad.cs.ubc.ca"
echo "Login using $login_username. Please enter your azure password"

az login -u $login_username

ids=$(az vm list -d --resource-group ServerResource --query "[?powerState=='VM running'].id" -o tsv)

echo "Stopping VM..."
echo $ids
START_TIME=$SECONDS
az vm stop --no-wait --ids $ids
ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo "Stopping VMs took $(($ELAPSED_TIME/60)) min $(($ELAPSED_TIME%60)) sec"

echo "Deallocation VM.."
START_TIME=$SECONDS
az vm deallocate --no-wait --ids $ids
ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo "Deallocting VMs took $(($ELAPSED_TIME/60)) min $(($ELAPSED_TIME%60)) sec"
