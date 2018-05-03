package main

import (
	"os"
	"fmt"
	"bufio"
)
import b64 "encoding/base64"

func main() {
	filePath := os.Args[1]
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()

	fscanner := bufio.NewScanner(f)
	for fscanner.Scan() {
		decodedArr, _ := b64.StdEncoding.DecodeString(fscanner.Text())
		stringWritten := string(decodedArr)
		fmt.Println(stringWritten)
	}
}