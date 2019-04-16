package main

import (
	"fmt"

	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-go-crawler/twse"
	"github.com/robfig/cron"
)

func init() {

	// config
	config.Init()

	// logger
	logger.Init(config.LogDirectory(), "crawler")

	// log config
	logger.Info(fmt.Sprintf("%s: %s", "EnvironmentName", config.EnvironmentName()))
	logger.Info(fmt.Sprintf("%s: %s", "LogDirectory", config.LogDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "DataDirectory", config.DataDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaBootstrapServers", config.KafkaBootstrapServers()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaProcessorTopic", config.KafkaProcessorTopic()))
	logger.Info(fmt.Sprintf("%s: %s", "CrawlerMode", config.CrawlerMode()))
	logger.Info(fmt.Sprintf("%s: %s", "CrawlerBatchKind", config.CrawlerBatchKind()))
	logger.Info(fmt.Sprintf("%s: %s", "CrawlerBatchStartTime", fmt.Sprint(config.CrawlerBatchStartTime())))
	logger.Info(fmt.Sprintf("%s: %s", "CrawlerBatchEndTime", fmt.Sprint(config.CrawlerBatchEndTime())))
	logger.Info(fmt.Sprintf("%s: %s", "TWSEURL", config.TWSEURL()))
	logger.Info(fmt.Sprintf("%s: %s", "TWSEReferer", config.TWSEReferer()))
	logger.Info(fmt.Sprintf("%s: %s", "TWSEDataDirectory", config.TWSEDataDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "TWSECron", config.TWSECron()))
}

var environmentName string

func process() {

	logger.Info("Starting process...")

	var err error

	// processor producer
	var processorProducer *kafka.Producer
	if processorProducer, err = kafka.NewProducer(config.KafkaBootstrapServers()); err != nil {
		logger.Panic(fmt.Sprintf("kafka.NewProducer %v", err))
	}
	defer processorProducer.Close()

	startTime := config.CrawlerBatchStartTime()
	endTime := config.CrawlerBatchEndTime()

	// twse
	err = twse.Process(
		config.CrawlerMode(),
		config.CrawlerBatchKind(),
		startTime.Year(),
		int(startTime.Month()),
		startTime.Day(),
		endTime.Year(),
		int(endTime.Month()),
		endTime.Day(),
		config.TWSEURL(),
		config.TWSEReferer(),
		config.TWSEDataDirectory(),
		processorProducer,
		config.KafkaProcessorTopic(),
	)
	if err != nil {
		logger.Panic(fmt.Sprintf("twse.Process %v", err))
	}

	processorProducer.Flush(5000)

	return
}

func main() {

	logger.Info("Starting crawler...")

	// environment name
	switch environmentName = config.EnvironmentName(); environmentName {
	case config.EnvironmentNameDev, config.EnvironmentNameTest, config.EnvironmentNameStg, config.EnvironmentNameProd:
	default:
		logger.Panic("Unknown environment name")
	}

	// mode
	switch mode := config.CrawlerMode(); mode {
	case config.CrawlerModeBatch:
		process()
	case config.CrawlerModeDaemon:
		// twse
		twseCron := cron.New()
		twseCron.AddFunc(config.TWSECron(), process)
		twseCron.Start()

		select {}
	default:
		logger.Panic("Unknown mode")
	}
}
