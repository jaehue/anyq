package anyq

import (
	"github.com/streadway/amqp"
	"testing"
)

func TestRabbitmqConn(t *testing.T) {
	q, err := New("rabbitmq", "amqp://guest:guest@127.0.0.1:5672/")
	if err != nil {
		t.Error(err)
	}

	conn, err := q.Conn()
	if err != nil {
		t.Error(err)
	}

	if _, ok := conn.(*amqp.Connection); !ok {
		t.Errorf("invalid consumer type(%T)\n", conn)
	}
}
