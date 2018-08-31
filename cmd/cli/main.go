package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"sync"

	"github.com/pagalguy/ds_to_json"
)

// This will crawl through the srcDir, read all datastore files, convert each entity into JSON
// and write the JSON to a file
// This will use workers to read parallely. Each worker will output one JSON file and one errors file.
func CrawlAndConvert(srcDir, destDir string) {

	// ensure and clean destDir
	ds_to_json.EnsureDir(destDir)
	ds_to_json.CleanDir(destDir)

	// collect files
	filesList, err := ds_to_json.WalkDir(srcDir)

	if err != nil {
		log.Fatalf("FATAL error while crawling input directory - %s: %v", srcDir, err)
	}

	log.Printf("Got %d files in %s", len(filesList), srcDir)

	var syncWg sync.WaitGroup

	//# of workers = cpu core count - 1 for the main go routine.
	numWorkers := int(math.Max(1.0, float64(runtime.NumCPU()-1)))

	// distribute the files among workers
	log.Printf("Starting %d workers...", numWorkers)

	for workerNum, batch := range chunk(filesList, numWorkers) {
		syncWg.Add(1)
		go func(workerNum int, batch []string) {
			RunWorker(workerNum, batch, destDir)
			syncWg.Done()
		}(workerNum, batch)
	}

	syncWg.Wait()

}

// Runs reading & writing logics in separate subroutines
func RunWorker(workerNum int, srcFiles []string, destDir string) error {

	log.Printf("[Worker #%d] Got %d files", workerNum, len(srcFiles))

	jsonChan := make(ds_to_json.JSONChan)
	errChan := make(ds_to_json.ErrorChan)

	var workerWg sync.WaitGroup

	go func() {
		for _, file := range srcFiles {
			log.Printf("[Worker #%d] Reading %s", workerNum, file)
			err := ds_to_json.ReadDatastoreFile(file, jsonChan, errChan)
			if err != nil {
				log.Printf("Error while reading file - %s: %v", file, err)
				continue
			}
		}
		close(jsonChan)
		close(errChan)
	}()

	// JSON objects writing subroutine
	workerWg.Add(1)
	go func() {

		jsonFilename := fmt.Sprintf("%s/converted-%d.json", destDir, workerNum)
		destJSONFile, err := os.OpenFile(jsonFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer destJSONFile.Close()

		if err != nil {
			log.Fatalf("FATAL Could not create an JSON output file - %s: %v", jsonFilename, err)
		}

		jsonWriter := bufio.NewWriter(destJSONFile)

		for destJSON := range jsonChan {
			serialized, err := json.Marshal(destJSON)
			if err != nil {
				log.Fatalf("Error while serialzing JSON %v \n %v", destJSON, err)
			}
			fmt.Fprintln(jsonWriter, string(serialized))
		}

		log.Printf("[Worker #%d] Completed writing JSON objects to %s", workerNum, jsonFilename)

		workerWg.Done()
	}()

	// Errors writing subroutine
	workerWg.Add(1)
	go func() {

		errFilename := fmt.Sprintf("%s/errors-%d.json", destDir, workerNum)
		destErrFile, err := os.OpenFile(errFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer destErrFile.Close()

		if err != nil {
			log.Fatalf("FATAL Could not create an errors output file - %s: %v", errFilename, err)
		}

		errWriter := bufio.NewWriter(destErrFile)

		for err := range errChan {
			errJson := map[string]interface{}{
				"message": err.Message,
				"file":    err.File,
				"line":    err.Line,
			}

			serialized, err := json.Marshal(errJson)

			if err != nil {
				log.Fatalf("Error while serialzing JSON %v \n %v", errJson, err)
			}

			fmt.Fprintln(errWriter, string(serialized))
		}

		log.Printf("[Worker #%d] Completed writing error objects to %s", workerNum, errFilename)

		workerWg.Done()

	}()

	workerWg.Wait()

	return nil
}

// Takes a strings list and returns all elements of `list` sliced into `max` number of slices
func chunk(list []string, max int) [][]string {
	chunked := make([][]string, 0)

	chunkSize := (len(list) + max - 1) / max

	for i := 0; i < len(list); i += chunkSize {
		end := i + chunkSize

		if end > len(list) {
			end = len(list)
		}

		chunked = append(chunked, list[i:end])
	}

	return chunked

}

func main() {
	flag.Parse()

	backupsFolder := flag.Arg(0)
	destFolder := flag.Arg(1)

	CrawlAndConvert(backupsFolder, destFolder)
}
