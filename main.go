package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-crawler/twse"
	"github.com/robfig/cron"
)

func init() {

	// log
	logfile, err := os.OpenFile("logfile.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("FATAL", "Open log file error:", err)
	}

	log.SetOutput(logfile)
	log.SetPrefix("CRAWLER ")
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	// log config
	log.Println("INFO", "Environment:", config.Environment())
	log.Println("INFO", "LogDirectory:", config.LogDirectory())
	log.Println("INFO", "DataDirectory:", config.DataDirectory())
	log.Println("INFO", "KafkaBootstrapServers:", config.KafkaBootstrapServers())
	log.Println("INFO", "KafkaProcessorTopic:", config.KafkaProcessorTopic())
	log.Println("INFO", "CrawlerMode:", config.CrawlerMode())
	log.Println("INFO", "CrawlerBatchStartTime:", fmt.Sprint(config.CrawlerBatchStartTime()))
	log.Println("INFO", "CrawlerBatchEndTime:", fmt.Sprint(config.CrawlerBatchEndTime()))
	log.Println("INFO", "CrawlerDaemonCron:", config.CrawlerDaemonCron())
	log.Println("INFO", "TWSEURL:", config.TWSEURL())
	log.Println("INFO", "TWSEReferer:", config.TWSEReferer())
	log.Println("INFO", "TWSEDataDirectory:", config.TWSEDataDirectory())
}

var environment, mode string

func process(start time.Time, end time.Time) (err error) {

	// processor producer
	var processorProducer *kafka.Producer
	if processorProducer, err = kafka.NewProducer(config.KafkaBootstrapServers()); err != nil {
		return
	}
	defer processorProducer.Close()

	for dt := start; !dt.After(end); dt = dt.AddDate(0, 0, 1) {

		isFinished := dt.Equal(end)

		if err = twse.Process(
			config.TWSEURL(),
			config.TWSEReferer(),
			dt,
			config.TWSEDataDirectory(),
			isFinished,
			processorProducer,
			config.KafkaProcessorTopic(),
		); err != nil {
			return
		}

		time.Sleep(10 * time.Second)
	}

	return
}

func batchProcess() {

	log.Println("INFO", "Starting batch process...")

	var err error

	var start, end time.Time

	if start, err = twse.GetCloseTime(config.CrawlerBatchStartTime()); err != nil {
		log.Panicln("PANIC", "GetCloseTime", err)
	}

	if end, err = twse.GetCloseTime(config.CrawlerBatchEndTime()); err != nil {
		log.Panicln("PANIC", "GetCloseTime", err)
	}

	if err = process(start, end); err != nil {
		log.Panicln("PANIC", "process", err)
	}
}

func daemonProcess() {

	log.Println("INFO", "Starting daemon process...")

	var err error

	var dt time.Time
	dt = time.Now()
	if dt, err = twse.GetCloseTime(dt.Year(), int(dt.Month()), dt.Day()); err != nil {
		log.Panicln("PANIC", "GetCloseTime", err)
	}

	if err = process(dt, dt); err != nil {
		log.Panicln("PANIC", "process", err)
	}
}

func main() {

	log.Println("INFO", "Starting crawler...")

	// environment
	environment = config.Environment()

	if environment != "dev" && environment != "test" && environment != "stg" && environment != "prod" {
		log.Panicln("PANIC", "Unknown environment")
	}

	// mode
	mode = config.CrawlerMode()

	if mode == "batch" {
		batchProcess()
	} else if mode == "daemon" {
		c := cron.New()
		c.AddFunc(config.CrawlerDaemonCron(), daemonProcess)
		c.Start()
		select {}
	} else {
		log.Panicln("PANIC", "Unknown mode")
	}
}
