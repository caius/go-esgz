package main

import "bufio"
import "bytes"
import "encoding/json"
import "fmt"
import "net/http"
import "os"
import "runtime"
import "time"

const BatchSize int = 2
const WorkerCount int = 2

var ESUrl string

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

func (doc *LogDocument) String() string {
	var buf bytes.Buffer

	buf.WriteString(doc.Header)
	buf.WriteString("\n")
	buf.WriteString(doc.Body)
	buf.WriteString("\n")

	return buf.String()
}

// Takes in log lines from a channel, parses the required info out & creates
// LogDocument instances for the upsertWorkers to insert into ES
func lineWorker(lines <-chan string, documents chan LogDocument, done chan bool) {
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

	done <- true
}

func upsertWorker(documents <-chan LogDocument, docCounter chan int, done chan bool) {
	docs := []LogDocument{}

	for doc := range documents {
		docs = append(docs, doc)

		if len(docs) >= BatchSize {
			upsertToES(docs)
			docCounter <- len(docs)
			docs = []LogDocument{} // reset array
		}
	}

	// Handle any remaining documents as well
	if len(docs) > 0 {
		upsertToES(docs)
	}

	// docCounter <- len(docs)
	done <- true
}

func upsertToES(documents []LogDocument) {
	body := new(bytes.Buffer)
	for _, doc := range documents {
		_, err := body.WriteString(doc.String())
		if err != nil {
			fmt.Println("Error upserting to ES")
			panic(err)
		}
	}

	resp, err := http.Post(ESUrl, "application/json", body)
	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()
	// fmt.Println(resp)
}

func outputStats(docCounter <-chan int, done chan bool) {
	totalCount := 0
	currentCount := 0

	startTime := time.Now()
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case i := <-docCounter:
			totalCount += i
			currentCount += i

		case <-ticker.C:
			c := currentCount
			currentCount = 0
			fmt.Printf("%d/s uploaded\n", c)

		case <-done:
			ticker.Stop()

			// Little summary to finish
			duration := time.Since(startTime).Seconds()
			fmt.Printf("Upserted %d documents in %.2f seconds at %.0f/s\n", totalCount, duration, float64(totalCount)/duration)

			return
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() + 1)

	// TODO: handle cli arguments better
	argv := os.Args[1:]
	indexName := argv[0]

	ESUrl = fmt.Sprintf("http://localhost:8080/%s/_bulk", indexName)
	fmt.Println(ESUrl)

	// TODO: make this a proper ARGF
	argf, err := os.Open("/dev/stdin")
	if err != nil {
		fmt.Println("Error reading stdin")
		panic(err)
	}

	// Setup our channels for dishing out/waiting on work
	lines := make(chan string)
	documents := make(chan LogDocument, 3)
	docCounter := make(chan int)

	statsDone := make(chan bool)
	lineDone := make(chan bool)
	upsertDone := make(chan bool)

	// Stats outputter
	go outputStats(docCounter, statsDone)

	// Kick off our worker routines
	for i := 0; i < WorkerCount; i++ {
		go lineWorker(lines, documents, lineDone)
		go upsertWorker(documents, docCounter, upsertDone)
	}

	// Read the input & buffer into LogDocuments for workers
	scanner := bufio.NewScanner(argf)
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

	close(docCounter)
	statsDone <- true
}
