package main

import (
	"../kafkaIO"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
)

var (
	kafkaIODirPath string
)

func main() {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("No caller information")
	}
	kafkaIODirPath = path.Dir(filename)

	id, _ := strconv.Atoi(os.Args[1])

	initFileSystem(id)

	for i := 0; i < 3; i++ {
		filePath := getFileName(id, i)
		data := []string{"a\n", "abc123!?$*&()'-=@~", "foo", "bar"}
		for _, s := range data {
			kafkaIO.AppendToFile(filePath, []byte(s))
		}
	}
}

func getFileName(id int, partitionID int) string {
	return filepath.Join(kafkaIODirPath, strconv.Itoa(id), strconv.Itoa(partitionID)+".kafka")
}

func initFileSystem(id int) {
	folderPath := filepath.Join(kafkaIODirPath, strconv.Itoa(id))
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		os.Mkdir(folderPath, 0700)
		fileArr := make([]*os.File, 3)
		for i := 0; i < 3; i++ {
			filePath := getFileName(id, i)
			file, _ := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
			fileArr[i] = file
			defer fileArr[i].Close()
		}
	}
}
