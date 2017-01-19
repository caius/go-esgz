package main

import "bufio"
import "encoding/json"
import "fmt"
import "strings"

const BatchSize int = 4
const WorkerCount int = 1

// Represents a bulk header & document to be inserted into ES
type LogDocument struct {
	Header string
	Body   string
}

// Used to parse the LogDocument Body to build the header for it
type LineInfo struct {
	UUID string `json:"id"` // `json:"@uuid"` // _id: json['@uuid'],
	// Type      string `json:"@type"` // _type: json['@type'],
	// Timestamp string `json:"@timestamp"` // _timestamp: json['@timestamp']
}

// Used to create the header JSON string for LogDocument
type LogHeader struct {
	Index     string `json:"_index"`
	Id        string `json:"_id"`
	Type      string `json:"_type"`
	Timestamp string `json:"_timestamp"`
}

// Takes in log lines from a channel, parses the required info out & creates
// LogDocument instances for the upsertWorkers to insert into ES
func lineWorker(lines <-chan string, documents chan LogDocument, completed chan bool) {
	for line := range lines {
		// Unpack the line for the bits we need to build the header
		lineInfo := LineInfo{}
		err := json.Unmarshal([]byte(line), &lineInfo)
		if err != nil {
			fmt.Println(line)
			fmt.Println(lineInfo)
			panic(err)
		}

		// Build a header now we have the info
		logHeader := LogHeader{Id: lineInfo.UUID}
		header, _ := json.Marshal(logHeader)

		// Create a LogDocument and queue it up
		document := LogDocument{Header: string(header), Body: line}
		documents <- document
	}

	completed <- true
}

func upsertWorker(documents <-chan LogDocument, completed chan bool) {
	docs := []LogDocument{}

	for doc := range documents {
		docs = append(docs, doc)

		if len(docs) >= BatchSize {
			upsertToES(docs)
			docs = []LogDocument{} // reset array
		}
	}

	// Handle any remaining documents as well
	if len(docs) > 0 {
		upsertToES(docs)
	}

	completed <- true
}

func upsertToES(documents []LogDocument) {
	fmt.Printf("worker got %d documents\n", len(documents))
	for _, doc := range documents {
		fmt.Printf("%s\n%s\n\n", doc.Header, doc.Body)
	}
}

func main() {
	// An artificial input source.  Normally this is a file passed on the command line.
	const input = `{"@uuid":"B92C2B19-900B-4385-9A04-CB97D4E7B05A","@type":"nginx"}
{"@uuid":"0F642EC2-2734-4E1B-A7A9-85F3D3AB01F8","@type":"nginx"}
{"@uuid":"6B090E82-29FA-4068-A377-B8E335A02391","@type":"app"}
{"@uuid":"CE8DEC80-4121-44C5-8887-49EF183558C2","@type":"app"}
{"@uuid":"62094485-69A6-4D1B-A05A-60989C9CC0A8","@type":"syslog"}
{"@uuid":"C27F4411-0548-4996-9065-17C1EB8250EC","@type":"syslog"}
{"@uuid":"55F2182C-4EB2-4A82-99B0-3092FBCDE750","@type":"app"}
{"@uuid":"9E25C76C-B0A5-4061-A7B2-842A6C3DD94A","@type":"nginx"}
{"@uuid":"308DEF68-EB81-4392-A545-C0D494B4FB85","@type":"nginx"}
`
	// Setup our channels for dishing out/waiting on work
	lines := make(chan string)
	documents := make(chan LogDocument, 3)
	lineDone := make(chan bool)
	upsertDone := make(chan bool)

	// Kick off our worker routines
	for i := 0; i < WorkerCount; i++ {
		go lineWorker(lines, documents, lineDone)
		go upsertWorker(documents, upsertDone)
	}

	// Read the input & buffer into LogDocuments for workers
	file := strings.NewReader(input)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
	close(lines)

	// Once the line workers are finished, close off documents channel
	for i := 0; i < WorkerCount; i++ {
		<-lineDone
	}
	close(documents)

	// Wait for upsert workers to do their thing
	for i := 0; i < WorkerCount; i++ {
		<-upsertDone
	}

}
