KAFKA LITE

Special instructions for compiling/running the code should be included in this file.

Start the Server:
go run server.go <- will print serverAddr to console

Run Brokers:
go run broker.go [serverAddr] [path to config.json] [brokerId] <- broker ids need to be unique

Example
go run broker.go 206.87.192.147:57829 config.json 1
go run broker.go 206.87.192.147:57829 config.json 2
go run broker.go 206.87.192.147:57829 config.json 3

Run Client App:
go run [client file].go [serverAddr]

Examples
go run producer_client.go 206.87.192.147:57829
go run producer_client2.go 206.87.192.147:57829
go run consumer_client.go [server-addr]

For our doggo app :) :
go run producer_doggo.go [serverAddr] <- prints the address its serving on to console

To Read Decoded Messages:
1) navigate to broker folder
2) run "go run readFileScript.go [filepath]"

Run Webapp:
open producer.html in browser, paste in the address of the doggo client, write a message, and click send

/** Sample Write and Read Workflow **/
1) 
	go run server.go --> assume it returns 206.87.192.147:57829
2) 
	go run broker.go 206.87.192.147:57829 config.json 1
	go run broker.go 206.87.192.147:57829 config.json 2
	go run broker.go 206.87.192.147:57829 config.json 3
3) 
	go run producer_client.go 206.87.192.147:57829

4)
	go run readFileScript.go 1/0.kafka

5) 
	go run consumer_client.go 206.87.192.147:57829