package main

import "bufio"
import "encoding/json"
import "fmt"
import "strings"

type LogDocument struct {
	Header string
	Body   string
}

func lineWorker(lines <-chan LogDocument, documents chan LogDocument, done chan bool) {
	for line := range lines {
		line.Header = "{}"
		documents <- line
	}

	done <- true
}

func upsertWorker(documents <-chan LogDocument, done chan bool) {
	for doc := range documents {
		fmt.Printf("%s -> %s\n", doc.Header, doc.Body)
	}

	done <- true
}

func main() {
	// An artificial input source.  Normally this is a file passed on the command line.
	const input = "{\"id\":5}\n{\"id\":10}\n{\"id\":15}\n{\"id\":20}\n{\"id\":25}\n"

	// How many workers we want
	const worker_count = 1

	// Setup our channels for dishing out/waiting on work
	lines := make(chan LogDocument)
	documents := make(chan LogDocument, 3)
	lineDone := make(chan bool)
	upsertDone := make(chan bool)

	// Kick off our worker routines
	for i := 0; i < worker_count; i++ {
		go lineWorker(lines, documents, lineDone)
		go upsertWorker(documents, upsertDone)
	}

	// Read the input & buffer into LogDocuments for workers
	file := strings.NewReader(input)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines <- LogDocument{Body: scanner.Text()}
	}
	close(lines)

	// Once the line workers are finished, close off documents channel
	for i := 0; i < worker_count; i++ {
		<-lineDone
	}
	close(documents)

	// Wait for upsert workers to do their thing
	for i := 0; i < worker_count; i++ {
		<-upsertDone
	}

}
