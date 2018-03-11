package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/marcoalmeida/ratelimiter"
)

const (
	// indexes for the arrays on the tables' objects
	src               = 0
	dst               = 1
	defaultMaxRetries = 3
	maxBatchSize      = 100
	defaultQps        = 10
)

type appConfig struct {
	region            [2]string
	endpoint          [2]string
	table             [2]string
	dynamo            [2]*dynamodb.DynamoDB
	partitionKey      string
	rangeKey          string
	maxConnectRetries int
	qps               int64
	verbose           bool
}

// because a modern language like Go does not need to support optional/required flags...
func checkFlags(app *appConfig) {
	if app.table[src] == "" || app.table[dst] == "" {
		log.Fatal("Need both DynamoDB tables")
	}

}

func backoff(i int, caller string) {
	wait := math.Pow(2, float64(i)) * 100
	log.Printf("%s: backing off for %f milliseconds\n", caller, wait)
	time.Sleep(time.Duration(wait) * time.Millisecond)
}

func verbose(cfg *appConfig, format string, a ...interface{}) {
	// it's convenient
	format += "\n"
	if cfg.verbose {
		log.Printf(format, a...)
	}
}

func connect(cfg *appConfig) {
	cfg.dynamo[src] = dynamodb.New(session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(cfg.region[src]).
				WithEndpoint(cfg.endpoint[src]).
				WithMaxRetries(cfg.maxConnectRetries),
		)))
	cfg.dynamo[dst] = dynamodb.New(session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(cfg.region[dst]).
				WithEndpoint(cfg.endpoint[dst]).
				WithMaxRetries(cfg.maxConnectRetries),
		)))
}

func validateTables(cfg *appConfig) error {
	var err error
	output := make([]*dynamodb.DescribeTableOutput, 2)

	for _, t := range []int{src, dst} {
		output[t], err = cfg.dynamo[t].DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(cfg.table[t]),
		})
		if err != nil {
			return err
		}
	}

	if !(reflect.DeepEqual(output[src].Table.AttributeDefinitions, output[dst].Table.AttributeDefinitions) &&
		reflect.DeepEqual(output[src].Table.KeySchema, output[dst].Table.KeySchema)) {
		msg := fmt.Sprintf(
			"Schema mismatch:\n**%s**\n%v\n**%s**\n%v\n",
			cfg.table[src],
			output[src],
			cfg.table[dst],
			output[dst])
		return errors.New(msg)
	}

	// save the primary key info (either table works, same schema)
	for _, k := range output[src].Table.KeySchema {
		if *k.KeyType == "HASH" {
			cfg.partitionKey = *k.AttributeName
		}
		if *k.KeyType == "RANGE" {
			cfg.rangeKey = *k.AttributeName
		}
	}

	return nil
}

func compareBatch(items []map[string]*dynamodb.AttributeValue, target int, cfg *appConfig) error {
	// create a list of items to issue the BatchGetItem request
	keys := make([]map[string]*dynamodb.AttributeValue, len(items))
	for i, item := range items {
		keys[i] = make(map[string]*dynamodb.AttributeValue, 0)
		keys[i][cfg.partitionKey] = item[cfg.partitionKey]
		if cfg.rangeKey != "" {
			keys[i][cfg.rangeKey] = item[cfg.rangeKey]
		}
	}

	responses := make([]map[string]*dynamodb.AttributeValue, 0)
	for len(keys) > 0 {
		input := &dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				cfg.table[target]: {
					Keys:           keys,
					ConsistentRead: aws.Bool(true),
				},
			}}
		output, err := cfg.dynamo[target].BatchGetItem(input)
		if err != nil {
			return err
		}
		//log.Println("out", output, "keys", keys)
		responses = append(responses, output.Responses[cfg.table[target]]...)
		keys = nil
		if len(output.UnprocessedKeys) > 0 {
			keys = output.UnprocessedKeys[cfg.table[target]].Keys
		}
	}

	// make sure row contents match
	for _, e1 := range items {
		exists := false
		for _, e2 := range responses {
			if reflect.DeepEqual(e1, e2) {
				exists = true
				break
			}
		}
		if !exists {
			log.Printf("Item not found %v in %s; ITEMS:\n%d\n", e1, cfg.table[target], items)
		}
	}

	return nil
}

// make sure every element in data exists in the target table
func testInclusionBatch(items []map[string]*dynamodb.AttributeValue, target int, cfg *appConfig) error {
	readRequest := make([]map[string]*dynamodb.AttributeValue, 0)
	rl := ratelimiter.New(cfg.qps)
	rl.Debug(cfg.verbose)

	for _, item := range items {
		requestSize := len(readRequest)
		if requestSize == maxBatchSize {
			rl.Acquire(maxBatchSize)
			err := compareBatch(readRequest, target, cfg)
			if err != nil {
				log.Println("Failed to compare batch:", err)
				log.Println(item)
			} else {
				verbose(cfg, "Successfully compared %d items", requestSize)
			}
			readRequest = []map[string]*dynamodb.AttributeValue{item}
		} else {
			readRequest = append(readRequest, item)
		}
	}

	// maybe len(items) % maxBatchSize != 0 and there is still something to process
	requestSize := len(readRequest)
	if requestSize > 0 {
		rl.Acquire(int64(requestSize))
		err := compareBatch(readRequest, target, cfg)
		if err != nil {
			log.Println("Failed to compare batch:", err)
		} else {
			verbose(cfg, "Successfully compared %d items", requestSize)
		}
	}

	return nil
}

// check for table inclusion, i.e., all elements of fst exist in snd
func testInclusion(fst int, snd int, cfg *appConfig) error {
	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)
	rl := ratelimiter.New(cfg.qps)
	rl.Debug(cfg.verbose)

	for {
		input := &dynamodb.ScanInput{
			TableName:      aws.String(cfg.table[fst]),
			ConsistentRead: aws.Bool(true),
		}
		// include the last key we received (if any) to resume scanning
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		successfulScan := false
		for i := 0; i < cfg.maxConnectRetries; i++ {
			rl.Acquire(1)
			result, err := cfg.dynamo[fst].Scan(input)
			if err != nil {
				backoff(i, "Scan")
			} else {
				successfulScan = true
				lastEvaluatedKey = result.LastEvaluatedKey
				verbose(cfg, "Scan: received %d items; last key:\n%v", len(result.Items), lastEvaluatedKey)
				testInclusionBatch(result.Items, snd, cfg)
				break
			}
		}

		if successfulScan {
			if len(lastEvaluatedKey) == 0 {
				// we're done
				verbose(cfg, "Scan: finished")
				return nil
			}
		} else {
			return errors.New(fmt.Sprintf("Scan: failed after %d attempts\n", cfg.maxConnectRetries))
		}
	}
}

func main() {
	cfg := &appConfig{}

	flag.StringVar(&cfg.region[src], "region1", "us-east-1", "AWS region of the first DynamoDB table")
	flag.StringVar(&cfg.region[1], "region2", "us-east-1", "AWS region of the second DynamoDB table")
	flag.StringVar(&cfg.table[src], "table1", "", "First DynamoDB table")
	flag.StringVar(&cfg.table[1], "table2", "", "Second DynamoDB table")
	flag.IntVar(
		&cfg.maxConnectRetries,
		"max-retries",
		defaultMaxRetries,
		"Maximum number of retries (with exponential backoff)",
	)
	flag.Int64Var(
		&cfg.qps,
		"qps",
		defaultQps,
		"Maximum queries per second",
	)
	flag.BoolVar(&cfg.verbose, "verbose", false, "Print verbose log messages")

	flag.Parse()

	checkFlags(cfg)

	connect(cfg)
	validateTables(cfg)
	testInclusion(src, dst, cfg)
	testInclusion(dst, src, cfg)
}
