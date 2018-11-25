package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"

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

	// config
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
	viper.SetConfigName("config") // name of config file (without extension)
	// viper.AddConfigPath("/etc/appname/")   // path to look for the config file in
	// viper.AddConfigPath("$HOME/.appname")  // call multiple times to add many search paths
	viper.AddConfigPath(".")   // optionally look for config in the working directory
	err = viper.ReadInConfig() // Find and read the config file
	if err != nil {            // Handle errors reading the config file
		log.Fatalln("FATAL", "Open config file error:", err)
	}

	// log config
	log.Println("INFO", "environment:", viper.GetString("environment"))
	log.Println("INFO", "mode:", viper.GetString("mode"))
	log.Println("INFO", "data.base:", viper.GetString("data.base"))
	log.Println("INFO", "kafka.bootstrap.servers:", viper.GetString("kafka.bootstrap.servers"))
	log.Println("INFO", "kafka.topics.processor:", viper.GetString("kafka.topics.processor"))
	log.Println("INFO", "batch.start.year:", viper.GetInt("batch.start.year"))
	log.Println("INFO", "batch.start.month:", viper.GetInt("batch.start.month"))
	log.Println("INFO", "batch.start.day:", viper.GetInt("batch.start.day"))
	log.Println("INFO", "batch.end.year:", viper.GetInt("batch.end.year"))
	log.Println("INFO", "batch.end.month:", viper.GetInt("batch.end.month"))
	log.Println("INFO", "batch.end.day:", viper.GetInt("batch.end.day"))
	log.Println("INFO", "daemon.cron:", viper.GetString("daemon.cron"))
	log.Println("INFO", "twse.url:", viper.GetString("twse.url"))
	log.Println("INFO", "twse.referer:", viper.GetString("twse.referer"))
	log.Println("INFO", "twse.data:", viper.GetString("twse.data"))
}

var environment, mode string

func process(start time.Time, end time.Time) (err error) {

	// processor topic
	var processorTopic string
	processorTopic = fmt.Sprintf("%s_%s", viper.GetString("kafka.topics.processor"), environment)

	// processor producer
	var processorProducer *kafka.Producer
	if processorProducer, err = kafka.NewProducer(viper.GetString("kafka.bootstrap.servers")); err != nil {
		return
	}
	defer processorProducer.Close()

	for dt := start; !dt.After(end); dt = dt.AddDate(0, 0, 1) {

		isFinished := dt.Equal(end)

		if err = twse.Process(
			viper.GetString("twse.url"),
			viper.GetString("twse.referer"),
			dt,
			viper.GetString("data.base")+viper.GetString("twse.data"),
			isFinished,
			processorProducer,
			processorTopic,
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

	if start, err = twse.GetCloseTime(viper.GetInt("batch.start.year"), time.Month(viper.GetInt("batch.start.month")), viper.GetInt("batch.start.day")); err != nil {
		log.Panicln("PANIC", "GetCloseTime", err)
	}

	if end, err = twse.GetCloseTime(viper.GetInt("batch.end.year"), time.Month(viper.GetInt("batch.end.month")), viper.GetInt("batch.end.day")); err != nil {
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
	if dt, err = twse.GetCloseTime(time.Now().Date()); err != nil {
		log.Panicln("PANIC", "GetCloseTime", err)
	}

	if err = process(dt, dt); err != nil {
		log.Panicln("PANIC", "process", err)
	}
}

func main() {

	log.Println("INFO", "Starting crawler...")

	// environment
	environment = viper.GetString("environment")

	if environment != "dev" && environment != "test" && environment != "stg" && environment != "prod" {
		log.Panicln("PANIC", "Unknown environment")
	}

	// mode
	mode = viper.GetString("mode")

	if mode == "batch" {
		batchProcess()
	} else if mode == "daemon" {
		c := cron.New()
		c.AddFunc(viper.GetString("daemon.cron"), daemonProcess)
		c.Start()
		select {}
	} else {
		log.Panicln("PANIC", "Unknown mode")
	}
}
