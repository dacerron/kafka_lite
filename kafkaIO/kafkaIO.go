package kafkaIO

import (
	"bufio"
	"fmt"
	"os"
)
import b64 "encoding/base64"

// this package includes helper for appending to a file and reading a certain offset from a file

func AppendToFile(filePath string, data []byte) error {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	sEnc := b64.StdEncoding.EncodeToString(data)
	_, err = f.WriteString(sEnc + "\n")
	if err != nil {
		return err
	}

	f.Sync()

	return nil
}

func BulkAppendToFile(filePath string, dataArr [][]byte) error {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, data := range dataArr {
		sEnc := b64.StdEncoding.EncodeToString(data)
		_, err = f.WriteString(sEnc + "\n")
		if err != nil {
			return err
		}
	}

	f.Sync()

	return nil
}

type OffsetOutOfRangeError uint

func (e OffsetOutOfRangeError) Error() string {
	return fmt.Sprintf("IO: Offset %d is out of range", uint(e))
}

func ReadAtOffset(filePath string, offset uint, N uint) ([][]byte, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var i uint = 0
	fscanner := bufio.NewScanner(f)
	var result [][]byte
	for fscanner.Scan() {
		// if within range of [offset...offset+N)
		if i >= offset && i < offset+N {
			sDec, _ := b64.StdEncoding.DecodeString(fscanner.Text())
			result = append(result, sDec)
		}
		// check if this will be the last item we read
		if i == offset+N-1 {
			return result, nil
		}
		i++
	}

	return result, OffsetOutOfRangeError(i)
}
