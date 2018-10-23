package main

import (
	"log"
	"time"

	bqs "github.com/lorenzosaino/go-bqstreamer"
	"google.golang.org/api/bigquery/v2"
)

func main() {
	done := make(chan struct{})
	defer close(done)

	// error handling goroutine
	errChan := make(chan *bqs.InsertErrors)
	go func() {
		for {
			err := <-errChan
			for _, table := range err.All() {
				for _, attempt := range table.Attempts() {
					if err := attempt.Error(); err != nil {
						log.Println(err)
					}
				}
			}
		}
	}()

	// read config
	jwtConfig, err := bqs.NewJWTConfig("bq.json")
	if err != nil {
		log.Fatalln(err)
	}

	// initialize a worker group.
	ipv4Only := true
	writer, err := bqs.NewAsyncWorkerGroup(
		jwtConfig,
		ipv4Only,
		bqs.SetAsyncNumWorkers(10),               // Number of background workers in the group.
		bqs.SetAsyncMaxRows(500),                 // Amount of rows that must be enqueued before executing an insert operation to BigQuery.
		bqs.SetAsyncMaxDelay(1*time.Second),      // Time to wait between inserts.
		bqs.SetAsyncRetryInterval(1*time.Second), // Time to wait between failed insert retries.
		bqs.SetAsyncMaxRetries(10),               // Maximum amount of retries a failed insert is allowed to be retried.
		bqs.SetAsyncIgnoreUnknownValues(true),    // Ignore unknown fields when inserting rows.
		bqs.SetAsyncSkipInvalidRows(true),        // Skip bad rows when inserting.
		bqs.SetAsyncErrorChannel(errChan),        // Set unified error channel.
	)

	if err != nil {
		log.Fatalln(err)
	}

	// start writer
	writer.Start()
	defer writer.Close()

	// enqueue a single row.
	rowData := make(map[string]bigquery.JsonValue)
	rowData["somekey"] = "a key"
	rowData["someotherkey"] = 1

	writer.Enqueue(
		bqs.NewRow(
			"my-project",
			"my-dataset",
			"my-table",
			rowData,
		))

	log.Println("write queued")
}
