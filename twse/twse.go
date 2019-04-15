package twse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/http"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	protobuf "github.com/joshchu00/finance-protobuf/inside"
)

var location *time.Location

func Init() {
	var err error
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		log.Fatalln("FATAL", "Get location error:", err)
	}
}

func getCloseTime(year int, month int, day int) int64 {
	return datetime.GetTimestamp(time.Date(year, time.Month(month), day, 13, 30, 0, 0, location))
}

func craw(kind string, ts int64, url string, referer string, dataDirectory string) (path string, err error) {

	logger.Info(fmt.Sprintf("%s: %s", "Starting twse craw...", datetime.GetTimeString(ts, location)))

	dateString := datetime.GetDateString(ts, location)

	path = fmt.Sprintf("%s/%s.json", dataDirectory, dateString)

	switch kind {
	case config.CrawlerBatchKindReal:

		var data []byte

		var valid bool

		for i := 0; i < 3 && !valid; i++ {

			if data, err = http.Get(fmt.Sprintf(url, dateString, datetime.GetTimestamp(time.Now())), referer); err != nil {
				return
			}

			valid = json.Valid(data)
		}

		if !valid {
			err = errors.New("Data is unavailable")
			return
		}

		if err = ioutil.WriteFile(path, data, 0644); err != nil {
			return
		}
	case config.CrawlerBatchKindVirtual:
	default:
		err = errors.New("Unknown batch kind")
		return
	}

	return
}

func Process(mode string, kind string, startTime time.Time, endTime time.Time, url string, referer string, dataDirectory string, producer *kafka.Producer, topic string) (err error) {

	logger.Info("Starting twse process...")

	var start, end int64

	switch mode {
	case config.CrawlerModeBatch:
		start = getCloseTime(startTime.Year(), int(startTime.Month()), startTime.Day())
		end = getCloseTime(endTime.Year(), int(endTime.Month()), endTime.Day())
	case config.CrawlerModeDaemon:
		kind = config.CrawlerBatchKindReal
		endTime = time.Now()
		end = getCloseTime(endTime.Year(), int(endTime.Month()), endTime.Day())
		start = end
	default:
		err = errors.New("Unknown mode")
		return
	}

	for ts := start; ts <= end; ts = datetime.AddOneDay(ts) {

		var path string

		path, err = craw(
			kind,
			ts,
			url,
			referer,
			dataDirectory,
		)
		if err != nil {
			return
		}

		message := &protobuf.Processor{
			Exchange:      "TWSE",
			Period:        "1d",
			Datetime:      ts,
			Path:          path,
			Last:          ts == end,
			FirstDatetime: start,
		}

		var bytes []byte

		bytes, err = proto.Marshal(message)
		if err != nil {
			return
		}

		producer.Produce(topic, 0, bytes)

		time.Sleep(10 * time.Second)
	}

	return
}
