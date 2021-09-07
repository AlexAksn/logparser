package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

var (
	reg           *regexp.Regexp
	inputPattern  = flag.String("in", "./*.log.gz", "filepath pattern")
	outputValid   = flag.String("outValid", "verified_googlebot_log_file.csv", "Verified googlebot log file name")
	outputInValid = flag.String("outInValid", "unverified_googlebot_log_file.csv", "Unverified googlebot log file name")
)

func init() {
	reg = regexp.MustCompile(`"ClientIP":"([\d\.]+)".+"ClientRequestHost":"([^"]+)".+"ClientRequestMethod":"([^"]+)".+"ClientRequestURI":"([^"]+)".+"EdgeEndTimestamp":"([^"]+)".+"EdgeResponseBytes":(\d+).+"EdgeResponseStatus":(\d+).+"ClientCountry":"([^"]+)".+"ClientRequestUserAgent":"([^"]+Googlebot[^"]+)"`)
}

func main() {
	//var pattern = "./*/20210803T155*.log.gz"
	//var pattern = "./*/20210803T155750Z_20210803T155820Z_63a33384.log.gz"
	flag.Parse()

	files, err := filepath.Glob(*inputPattern)
	if err != nil {
		panic(err)
	}

	filesCh := make(chan string, 100)
	successCh := make(chan string)
	invalidCh := make(chan string)
	doneSuccess := make(chan bool)
	doneInvalid := make(chan bool)

	wg := sync.WaitGroup{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go worker(filesCh, successCh, invalidCh, &wg)
	}

	go func() {
		for _, fn := range files {
			filesCh <- fn
		}
		fmt.Println("files added to the channel:", len(files))
		close(filesCh)
	}()

	go consume(*outputValid, successCh, doneSuccess)
	go consume(*outputInValid, invalidCh, doneInvalid)

	go func() {
		wg.Wait()
		close(successCh)
		close(invalidCh)
	}()

	successResult := <-doneSuccess
	logFileSaveResult(successResult, *outputValid)
	invalidResult := <-doneInvalid
	logFileSaveResult(invalidResult, *outputInValid)
}

func consume(filename string, dataCh chan string, done chan bool) {
	f, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Fprintln(f, "\"Id\",\"Host\",\"Date Time\",\"Method\",\"URL\",\"Response Code\",\"Bytes Sent\",\"User Agent\"")
	for l := range dataCh {
		_, err = fmt.Fprintln(f, l)
		if err != nil {
			fmt.Println(err)
			f.Close()
			done <- false
			return
		}
	}
	err = f.Close()
	if err != nil {
		fmt.Println(err)
		done <- false
		return
	}
	done <- true
}

func worker(filesCh <-chan string, successCh chan<- string, invalidCh chan<- string, wg *sync.WaitGroup) {
	for fn := range filesCh {

		err := processFile(fn, successCh, invalidCh)
		if err != nil {
			fmt.Printf("Error: %s\n", err.Error())
		}
	}
	wg.Done()
}

func processFile(filename string, successCh chan<- string, invalidCh chan<- string) (err error) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return errors.Wrap(err, "Failed to open file")
	}

	gzreader, err := gzip.NewReader(file)
	if err != nil {
		return errors.Wrap(err, "Failed gzip.NewReader")
	}
	txt, err := ioutil.ReadAll(gzreader)
	if err != nil {
		return errors.Wrap(err, "Failed ioutil.ReadAll")
	}

	res := reg.FindAllSubmatch(txt, -1)
	for _, r := range res {
		host := r[1]
		requestHost := r[2]
		method := r[3]
		requestUri := r[4]
		timestamp := r[5]
		bytesd := r[6]
		code := r[7]
		ua := r[9]
		line := fmt.Sprintf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s%s\",\"%s\",\"%s\",\"%s\"", filename, host, timestamp, method, requestHost, requestUri, code, bytesd, ua)

		if isGoogleBot(host) {
			successCh <- line
		} else {
			invalidCh <- line
		}
	}
	return nil
}

func isGoogleBot(host []byte) (result bool) {
	names, err := net.LookupAddr(string(host))
	if err != nil {
		return false
	}

	for _, name := range names {
		if strings.Contains(name, "googlebot") {
			return true
		}
	}

	return false
}

func logFileSaveResult(res bool, filename string) {
	if res {
		fmt.Printf("File written successfully: %s\n", filename)
	} else {
		fmt.Printf("File writing failed: %s\n", filename)
	}
}
