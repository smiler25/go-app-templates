package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/logutils"
	"github.com/jessevdk/go-flags"
	"github.com/streadway/amqp"
)

//var dateLayout = "2006-01-02T15:04:05.000Z"

type Message struct {
	Datetime string `json:"datetime"`
	Field    string `json:"field"`
	State    bool   `json:"state"`
}

type Opts struct {
	Dbg           bool       `long:"debug" env:"DEBUG" description:"debug mode"`
	Rabbit        RabbitOpts `group:"rabbit" namespace:"rabbit" env-namespace:"RABBIT"`
	NumberWorkers int        `long:"workers" env:"WORKERS" description:"number workers" default:"1"`
}

// RabbitOpts holds all rabbit params
type RabbitOpts struct {
	Host      string `long:"host" env:"HOST" description:"rabbit host"`
	Port      int    `long:"port" env:"PORT" description:"rabbit port" default:"5672"`
	User      string `long:"user" env:"USER" description:"rabbit user"`
	Password  string `long:"password" env:"PASSWORD" description:"rabbit password"`
	Queue     string `long:"queue" env:"QUEUE" description:"rabbit queue"`
	Reconnect int    `long:"reconnect" env:"RECONNECT" description:"rabbit reconnect delay" default:"30"`
	Prefetch  int    `long:"prefetch" env:"PREFETCH" description:"rabbit prefetch count" default:"5"`
}

func RabbitWorker(opts *Opts) {
	var err error

	reconnectDelay := time.Duration(opts.Rabbit.Reconnect) * time.Second
	for {
		err = consume(opts.Rabbit, opts.NumberWorkers)
		if err != nil {
			log.Printf("[ERROR] consume error %+v reconnect at %+v", err.Error(), time.Now().Local().Add(reconnectDelay))
		} else {
			log.Printf("[ERROR] consume stopped reconnect at %+v", time.Now().Local().Add(reconnectDelay))
		}
		time.Sleep(reconnectDelay)
	}
}

func consume(opts RabbitOpts, numWorkers int) (err error) {
	var rabbitConn *amqp.Connection
	var rabbitAddr = fmt.Sprintf("amqp://%s:%s@%s:%d/", opts.User, opts.Password, opts.Host, opts.Port)
	rabbitConn, err = amqp.Dial(rabbitAddr)

	if err != nil {
		log.Printf("[ERROR] rabbit connect error %+v", err.Error())
		return err
	}

	rabbitChan, err := rabbitConn.Channel()
	if err != nil {
		log.Printf("[ERROR] rabbit open channel error %+v", err.Error())
		return err
	}

	q, err := rabbitChan.QueueDeclare(
		opts.Queue, // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		log.Printf("[ERROR] rabbit QueueDeclare error %+v", err.Error())
		return err
	}

	err = rabbitChan.Qos(
		opts.Prefetch, // prefetch count
		0,             // prefetch size
		false,         // global
	)
	if err != nil {
		log.Printf("[ERROR] rabbit set Qos error %+v", err.Error())
		return err
	}

	tasks, err := rabbitChan.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Printf("[ERROR] rabbit register consumer error %+v", err.Error())
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	log.Printf("[INFO] running consumer host=%s queue=%s workers=%d", opts.Host, opts.Queue, numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker(tasks)
	}

	go func() {
		log.Printf("[ERROR] rabbit connection closed: %+v", <-rabbitConn.NotifyClose(make(chan *amqp.Error)))
		wg.Done()
	}()

	wg.Wait()

	return err
}

func worker(tasks <-chan amqp.Delivery) {
	for taskItem := range tasks {
		msg := &Message{}
		err := json.Unmarshal(taskItem.Body, msg)
		if err != nil {
			log.Printf("[ERROR] worker cant unpack json %+v", err)
			taskItem.Ack(false)
			continue
		}
		handleMessage(msg)
		taskItem.Ack(false)
	}
}

func handleMessage(m *Message) {
	//t, err := time.Parse(dateLayout, m.Datetime)
	//if err != nil {
	//	log.Printf("[ERROR] error parsing date msg=%+v, error=%+v", m, err)
	//	t = time.Now()
	//}
	log.Printf("[INFO] message consumed msg=%+v", m)
}

func setupLog(dbg bool) {
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR"},
		MinLevel: logutils.LogLevel("INFO"),
		Writer:   os.Stdout,
	}

	log.SetFlags(log.Ldate | log.Ltime)

	if dbg {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
		filter.MinLevel = logutils.LogLevel("DEBUG")
	}
	log.SetOutput(filter)
}

func main() {
	var opts Opts
	p := flags.NewParser(&opts, flags.Default)
	if _, e := p.ParseArgs(os.Args[1:]); e != nil {
		os.Exit(1)
	}
	setupLog(true)
	RabbitWorker(&opts)
}
