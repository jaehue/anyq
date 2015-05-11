# rabbitmq-consumer

A simple command line tool to consume a queue and print the
messages on the standard output.

### Usage

```
$ ./rabbitmq-consumer -help
Usage of ./rabbitmq-consumer:
  -consumer-tag="simple-consumer": AMQP consumer tag (should not be blank)
  -exchange="test-exchange": Durable, non-auto-deleted AMQP exchange name
  -exchange-type="direct": Exchange type - direct|fanout|topic|x-custom
  -key="test-key": AMQP binding key
  -queue="test-queue": Ephemeral AMQP queue name
  -uri="amqp://guest:guest@localhost:5672/": AMQP URI
```

```
$ ./rabbitmq-consumer -uri=amqp://guest:guest@localhost:5672/ -queue=test
2015/05/11 16:14:59 dialing amqp  amqp://guest:guest@localhost:5672/
2015/05/11 16:14:59 got Connection, getting Channel
2015/05/11 16:14:59 got Channel
2015/05/11 16:14:59 declaring Exchange:  test-exchange
2015/05/11 16:14:59 declared Exchange
2015/05/11 16:14:59 declaring Queue:  test
2015/05/11 16:14:59 declared Queue ("test" 0 messages, 0 consumers)
2015/05/11 16:14:59 binding to Exchange (key "test-key")
2015/05/11 16:14:59 Queue bound to Exchange
2015/05/11 16:14:59 starting consume
[receive] hello1
[receive] hello2
^C2015/05/11 16:15:43 cleaning...
2015/05/11 16:15:43 Rabbitmq shutdown OK
2015/05/11 16:15:43 clean complate
```