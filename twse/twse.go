package twse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/http"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-protobuf"
)

var location *time.Location

func Init() {
	var err error
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		log.Fatalln("FATAL", "Get location error:", err)
	}
}

func GetCloseTime(year int, month int, day int) int64 {
	return datetime.GetTimestamp(time.Date(year, time.Month(month), day, 13, 30, 0, 0, location))
}

func Process(kind string, url string, referer string, ts int64, path string, isFinished bool, producer *kafka.Producer, topic string) (err error) {

	logger.Info(fmt.Sprintf("%s: %s", "Starting twse process...", datetime.GetTimeString(ts, location)))

	dateString := datetime.GetDateString(ts, location)

	path = fmt.Sprintf("%s/%s.json", path, dateString)

	if kind == "real" {

		var data []byte

		var valid bool

		for i := 0; i < 3 && !valid; i++ {

			if data, err = http.Get(fmt.Sprintf(url, dateString, datetime.GetTimestamp(time.Now())), referer); err != nil {
				return
			}

			valid = json.Valid(data)
		}

		if !valid {
			err = errors.New("Data is unabailable")
			return
		}

		if err = ioutil.WriteFile(path, data, 0644); err != nil {
			return
		}
	} else if kind == "virtual" {

	} else {
		err = errors.New("Unknown batch kind")
		return
	}

	message := &protobuf.Processor{
		Exchange:   "TWSE",
		Period:     "1d",
		Datetime:   ts,
		Path:       path,
		IsFinished: isFinished,
	}

	var bytes []byte

	if bytes, err = proto.Marshal(message); err != nil {
		return
	}

	producer.Produce(topic, 0, bytes)

	return
}
