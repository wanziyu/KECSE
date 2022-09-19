package kafka_test

import (
	"fmt"
	sarama "github.com/Shopify/sarama"
	"log"
	"time"
)

func main() {
	config := sarama.NewConfig()
	// request.timeout.ms
	config.Producer.Timeout = time.Second * 5
	// message.max.bytes
	config.Producer.MaxMessageBytes = 1024 * 1024
	// request.required.acks
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.V0_11_0_1

	if err := config.Validate(); err != nil {
		panic(fmt.Errorf("invalid configuration, error: %v", err))
	}

	producer, err := sarama.NewSyncProducer([]string{"10.116.9.37:9092"}, config)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	for {
		msg := &sarama.ProducerMessage{
			Topic: "topic-C",
			Value: sarama.StringEncoder("this is a testing message from sarama!!"),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("FAILED to send message: %s\n", err)
		} else {
			log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
