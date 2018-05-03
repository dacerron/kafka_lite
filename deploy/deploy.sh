#!/bin/bash

source ./start_vms.sh
proj_path="~/go/src/stash.ugrad.cs.ubc.ca/"

echo "Cloning Repository..."
ips=$(az network public-ip list -g ServerResource --query "[].ipAddress" -o tsv)

echo "Please enter your stash password"
read -s password

urlencode() {
  # urlencode <string>
  local length="${#1}"
  for (( i = 0; i < length; i++ )); do
    local c="${1:i:1}"
    case $c in
      [a-zA-Z0-9.~_-]) printf "$c" ;;
      *) printf '%%%02X' "'$c"
    esac
  done
}

encoded=$(urlencode $password)

while read -r ip; do
  ssh -T -o "StrictHostKeyChecking no" "j9x9a@$ip" << ENDHERE
  cd $proj_path
  rm -rf proj2_j9x9a_l9z9a_q7z9a_z1e9
git clone https://$csid:$encoded@stash.ugrad.cs.ubc.ca:8443/scm/cs416_2017w2_/proj2_j9x9a_l9z9a_q7z9a_z1e9.git
   exit
ENDHERE
done <<< "$ips"

read -n1 -p "Would you like to stop and deallocate vm? [y,n]" doit
case $doit in
  y|Y) echo "\nStopping VM\n";
    ids=$(az vm list -d --resource-group ServerResource --query "[?powerState=='VM running'].id" -o tsv)
    az vm stop --ids $ids
    az vm deallocate --ids $ids ;;
  n|N) echo "\nPlease remember to run stop_vms.sh when you are done\n" ;;
  *) echo dont know ;;
esac
