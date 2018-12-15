package main

import (
	"fmt"
	"time"

	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-crawler/twse"
	"github.com/robfig/cron"
)

func init() {

	// config
	config.Init()

	// logger
	logger.Init(config.LogDirectory(), "crawler")

	// log config
	logger.Info(fmt.Sprintf("%s: %s", "Environment", config.Environment()))
	logger.Info(fmt.Sprintf("%s: %s", "LogDirectory", config.LogDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "DataDirectory", config.DataDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaBootstrapServers", config.KafkaBootstrapServers()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaProcessorTopic", config.KafkaProcessorTopic()))
	logger.Info(fmt.Sprintf("%s: %s", "CrawlerMode", config.CrawlerMode()))
	logger.Info(fmt.Sprintf("%s: %s", "CrawlerBatchMode", config.CrawlerBatchMode()))
	logger.Info(fmt.Sprintf("%s: %s", "CrawlerBatchStartTime", fmt.Sprint(config.CrawlerBatchStartTime())))
	logger.Info(fmt.Sprintf("%s: %s", "CrawlerBatchEndTime", fmt.Sprint(config.CrawlerBatchEndTime())))
	logger.Info(fmt.Sprintf("%s: %s", "TWSEURL", config.TWSEURL()))
	logger.Info(fmt.Sprintf("%s: %s", "TWSEReferer", config.TWSEReferer()))
	logger.Info(fmt.Sprintf("%s: %s", "TWSEDataDirectory", config.TWSEDataDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "TWSECron", config.TWSECron()))

	// twse
	twse.Init()
}

var environment string

func process(mode string, start int64, end int64) (err error) {

	// processor producer
	var processorProducer *kafka.Producer
	if processorProducer, err = kafka.NewProducer(config.KafkaBootstrapServers()); err != nil {
		return
	}
	defer processorProducer.Close()

	for ts := start; ts <= end; ts = datetime.AddOneDay(ts) {

		if err = twse.Process(
			mode,
			config.TWSEURL(),
			config.TWSEReferer(),
			ts,
			config.TWSEDataDirectory(),
			ts == end,
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

	logger.Info("Starting batch process...")

	start := twse.GetCloseTime(config.CrawlerBatchStartTime())
	end := twse.GetCloseTime(config.CrawlerBatchEndTime())

	if err := process(config.CrawlerBatchMode(), start, end); err != nil {
		logger.Panic(fmt.Sprintf("process %v", err))
	}
}

func daemonProcess() {

	logger.Info("Starting daemon process...")

	dt := time.Now()
	ts := twse.GetCloseTime(dt.Year(), int(dt.Month()), dt.Day())

	if err := process("real", ts, ts); err != nil {
		logger.Panic(fmt.Sprintf("process %v", err))
	}
}

func main() {

	logger.Info("Starting crawler...")

	// environment
	environment = config.Environment()

	if environment != "dev" && environment != "test" && environment != "stg" && environment != "prod" {
		logger.Panic("Unknown environment")
	}

	// mode
	mode := config.CrawlerMode()

	if mode == "batch" {
		batchProcess()
	} else if mode == "daemon" {
		// twse
		twseCron := cron.New()
		twseCron.AddFunc(config.TWSECron(), daemonProcess)
		twseCron.Start()

		select {}
	} else {
		logger.Panic("Unknown mode")
	}
}
