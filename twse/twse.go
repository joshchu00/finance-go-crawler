package twse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/data"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/http"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	protobuf "github.com/joshchu00/finance-protobuf/inside"
)

func getCloseTime(year int, month int, day int, location *time.Location) int64 {
	return datetime.GetTimestamp(time.Date(year, time.Month(month), day, 13, 30, 0, 0, location))
}

func craw(url string, dateString string, referer string, path string) (err error) {

	logger.Info(fmt.Sprintf("%s: %s", "Starting twse craw...", path))

	var bytes []byte

	var valid bool

	for i := 0; i < 3 && !valid; i++ {

		if bytes, err = http.Get(fmt.Sprintf(url, dateString, datetime.GetTimestamp(time.Now())), referer); err != nil {
			return
		}

		valid = json.Valid(bytes)
	}

	if !valid {
		err = errors.New("Data is unavailable")
		return
	}

	if err = ioutil.WriteFile(path, bytes, 0644); err != nil {
		return
	}

	return
}

func Process(
	mode string,
	kind string,
	startTimeYear int,
	startTimeMonth int,
	startTimeDay int,
	endTimeYear int,
	endTimeMonth int,
	endTimeDay int,
	url string,
	referer string,
	dataDirectory string,
	producer *kafka.Producer,
	topic string,
) (err error) {

	logger.Info("Starting twse process...")

	var location *time.Location
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		return
	}

	var start, end int64

	switch mode {
	case config.CrawlerModeBatch:
		start = getCloseTime(startTimeYear, int(startTimeMonth), startTimeDay, location)
		end = getCloseTime(endTimeYear, int(endTimeMonth), endTimeDay, location)
	case config.CrawlerModeDaemon:
		kind = config.CrawlerBatchKindReal
		startTime := time.Now().In(location)
		start = getCloseTime(startTime.Year(), int(startTime.Month()), startTime.Day(), location)
		end = start
	default:
		err = errors.New("Unknown mode")
		return
	}

	switch kind {
	case config.CrawlerBatchKindReal:
		for ts := start; ts <= end; ts = datetime.AddOneDay(ts) {

			dateString := datetime.GetDateString(ts, location)
			path := data.GetPath(dataDirectory, dateString)

			err = craw(
				url,
				dateString,
				referer,
				path,
			)
			if err != nil {
				return
			}

			time.Sleep(10 * time.Second)
		}
	case config.CrawlerBatchKindVirtual:
	default:
		err = errors.New("Unknown kind")
		return
	}

	message := &protobuf.Processor{
		Exchange:      "TWSE",
		Period:        "1d",
		StartDatetime: start,
		EndDatetime:   end,
	}

	var bytes []byte

	bytes, err = proto.Marshal(message)
	if err != nil {
		return
	}

	producer.Produce(topic, 0, bytes)

	return
}
