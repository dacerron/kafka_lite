echo "List of Public IPs:"
az network public-ip list -g ServerResource --query "[].{Name:name, publicIp:ipAddress}"
