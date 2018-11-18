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
	"github.com/joshchu00/finance-protobuf"
)

func GetCloseTime(year int, month time.Month, day int) (t time.Time, err error) {

	var location *time.Location
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		return
	}

	t = time.Date(year, month, day, 13, 30, 0, 0, location)

	return
}

func Process(url string, referer string, t time.Time, path string, isFinished bool, producer *kafka.Producer, topic string) (err error) {

	log.Println("INFO", "Starting twse process...", t.String())

	var bytes []byte

	dateString := datetime.GetDateString(t)

	var valid bool

	for i := 0; i < 3 && !valid; i++ {

		if bytes, err = http.Get(fmt.Sprintf(url, dateString, datetime.GetMilliSecond(time.Now())), referer); err != nil {
			return
		}

		valid = json.Valid(bytes)
	}

	if !valid {
		err = errors.New("Data is unabailable")
		return
	}

	path = fmt.Sprintf("%s/%s.json", path, dateString)

	if err = ioutil.WriteFile(path, bytes, 0644); err != nil {
		return
	}

	message := &protobuf.Processor{
		Exchange:   "TWSE",
		Datetime:   datetime.GetMilliSecond(t),
		Path:       path,
		IsFinished: isFinished,
	}

	bytes, err = proto.Marshal(message)
	if err != nil {
		return
	}
	producer.Produce(topic, 0, bytes)

	return
}
