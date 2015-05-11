# rabbitmq-producer

A simple command line tool to produce a single message to Rabbitmq

### Usage

```
$ ./rabbitmq-producer -help
Usage of ./rabbitmq-producer:
  -body="foobar": Body of message
  -exchange="test-exchange": Durable AMQP exchange name
  -exchange-type="direct": Exchange type - direct|fanout|topic|x-custom
  -key="test-key": AMQP routing key
  -reliable=true: Wait for the publisher confirmation before exiting
  -uri="amqp://guest:guest@localhost:5672/": AMQP URI
```

```
$ ./rabbitmq-producer -uri=guest:guest@localhost:5672/ -body=hello1
2015/05/11 16:23:41 dialing amqp  guest:guest@localhost:5672/
2015/05/11 16:23:41 got Connection, getting Channel
2015/05/11 16:23:41 got Channel
2015/05/11 16:23:41 declaring Exchange:  test-exchange
2015/05/11 16:23:41 declared Exchange
2015/05/11 16:23:41 enabling publishing confirms.
[send] hello1
2015/05/11 16:23:41 Rabbitmq shutdown OK

$ ./rabbitmq-producer -uri=guest:guest@localhost:5672/ -body=hello2
2015/05/11 16:23:43 dialing amqp  guest:guest@localhost:5672/
2015/05/11 16:23:43 got Connection, getting Channel
2015/05/11 16:23:43 got Channel
2015/05/11 16:23:43 declaring Exchange:  test-exchange
2015/05/11 16:23:43 declared Exchange
2015/05/11 16:23:43 enabling publishing confirms.
[send] hello2
2015/05/11 16:23:43 Rabbitmq shutdown OK
```