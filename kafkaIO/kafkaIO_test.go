package kafkaIO

import (
	"bufio"
	"os"
	"testing"
)

import b64 "encoding/base64"

func TestAppendToFile(t *testing.T) {
	var filePath string = "testing"

	err := AppendToFile(filePath, []byte{})
	if !os.IsNotExist(err) {
		t.Errorf("Expected File doesn't exist error")
	}

	f, err := os.Create(filePath)
	defer f.Close()
	defer os.Remove(filePath)
	data := []string{"a\n", "abc123!?$*&()'-=@~", "foo", "bar"}

	for _, s := range data {
		bArr := []byte(s)
		err = AppendToFile(filePath, bArr)
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}
	}

	i := 0
	fscanner := bufio.NewScanner(f)
	for fscanner.Scan() {
		decodedArr, _ := b64.StdEncoding.DecodeString(fscanner.Text())
		stringWritten := string(decodedArr)
		if stringWritten != data[i] {
			t.Errorf("Invalid string written.  Expected: %s , Actual: %s", data[i], stringWritten)
		}
		i++
	}
}

func TestBulkAppendToFile(t *testing.T) {
	var filePath string = "testing_bulk_write"

	err := BulkAppendToFile(filePath, [][]byte{})
	if !os.IsNotExist(err) {
		t.Errorf("Expected File doesn't exist error")
	}

	f, err := os.Create(filePath)
	defer f.Close()
	defer os.Remove(filePath)
	data := []string{"a\n", "abc123!?$*&()'-=@~", "foo", "bar"}

	var bArr [][]byte
	for _, s := range data {
		bArr = append(bArr, []byte(s))
	}

	err = BulkAppendToFile(filePath, bArr)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	i := 0
	fscanner := bufio.NewScanner(f)
	for fscanner.Scan() {
		decodedArr, _ := b64.StdEncoding.DecodeString(fscanner.Text())
		stringWritten := string(decodedArr)
		if stringWritten != data[i] {
			t.Errorf("Invalid string written.  Expected: %s , Actual: %s", data[i], stringWritten)
		}
		i++
	}
}

func TestReadOffset(t *testing.T) {
	var filePath string = "testing2"

	_, err := ReadAtOffset(filePath, 0, 1)
	if !os.IsNotExist(err) {
		t.Errorf("Expected File doesn't exist error")
	}

	f, err := os.Create(filePath)
	defer f.Close()
	defer os.Remove(filePath)

	data := []string{"a\n", "abc123!?$*&()'-=@~", "foo", "bar"}
	for _, s := range data {
		sEnc := b64.StdEncoding.EncodeToString([]byte(s))
		f.WriteString(sEnc + "\n")
	}

	for i, s := range data {
		var offset uint = uint(i)
		bArr, err := ReadAtOffset(filePath, offset, 1)
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}
		if string(bArr[0]) != s {
			t.Errorf("Invalid string read.  Expected: %s , Actual: %s", s, string(bArr[0]))
		}
	}

	_, err = ReadAtOffset(filePath, 1000, 1)

	if err != nil {
		switch err.(type) {
		case OffsetOutOfRangeError:
		default:
			t.Errorf("Expected OffsetOutOfRangeError")
		}
	} else {
		t.Errorf("Expected OffsetOutOfRangeError")
	}

}

func TestReadOffsetMultipleMsg(t *testing.T) {
	var filePath string = "testing3"

	_, err := ReadAtOffset(filePath, 0, 1)
	if !os.IsNotExist(err) {
		t.Errorf("Expected File doesn't exist error")
	}

	f, err := os.Create(filePath)
	defer f.Close()
	defer os.Remove(filePath)

	data := []string{"a\n", "abc123!?$*&()'-=@~", "foo", "bar"}
	for _, s := range data {
		sEnc := b64.StdEncoding.EncodeToString([]byte(s))
		f.WriteString(sEnc + "\n")
	}

	// Read 3 messages starting at offset 0
	var offset uint = uint(0)
	var N uint = uint(3)
	bArr, err := ReadAtOffset(filePath, offset, N)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if string(bArr[0]) != data[0] {
		t.Errorf("Invalid string read.  Expected: %s , Actual: %s", data[0], string(bArr[0]))
	}
	if string(bArr[1]) != data[1] {
		t.Errorf("Invalid string read.  Expected: %s , Actual: %s", data[1], string(bArr[1]))
	}
	if string(bArr[2]) != data[2] {
		t.Errorf("Invalid string read.  Expected: %s , Actual: %s", data[2], string(bArr[2]))
	}
	if len(bArr) != int(N) {
		t.Errorf("Invalid number of msgs returned. Expected: len(bArr)=%d , Actual: len(bArr)=%d", N, len(bArr))
	}

	// Read 2 messages starting at offset 1
	offset = uint(1)
	N = uint(2)
	bArr, err = ReadAtOffset(filePath, offset, N)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if string(bArr[0]) != data[1] {
		t.Errorf("Invalid string read.  Expected: %s , Actual: %s", data[1], string(bArr[0]))
	}
	if string(bArr[1]) != data[2] {
		t.Errorf("Invalid string read.  Expected: %s , Actual: %s", data[2], string(bArr[1]))
	}
	if len(bArr) != int(N) {
		t.Errorf("Invalid number of msgs returned. Expected: len(bArr)=%d , Actual: len(bArr)=%d", N, len(bArr))
	}

	_, err = ReadAtOffset(filePath, 1000, 1)

	if err != nil {
		switch err.(type) {
		case OffsetOutOfRangeError:
		default:
			t.Errorf("Expected OffsetOutOfRangeError")
		}
	} else {
		t.Errorf("Expected OffsetOutOfRangeError")
	}

}

// CLEANUP Test Files
func TestCleanup(t *testing.T) {
	os.Remove("testing")
	os.Remove("testing2")
	os.Remove("testing3")
}
