package anyq

import (
	"github.com/apcera/nats"
	"log"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestNatsReply(t *testing.T) {
	q, err := New("nats", "nats://127.0.0.1:4222")
	if err != nil {
		panic(err)
	}

	// reflect nats Conn
	v := reflect.ValueOf(q)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	conn := v.FieldByName("Conn")
	natsConn, ok := conn.Interface().(*nats.Conn)
	if !ok {
		log.Fatalln("invalid conn type", conn)
	}

	// set consumer for reply
	natsConn.Subscribe("test", func(m *nats.Msg) {
		natsConn.Publish(m.Reply, m.Data)
		log.Println("[receive and reply]", string(m.Data))
	})

	mmap := map[int]int{}

	// set producer for request
	for i := 0; i < 1000; i++ {
		go func(i int) {
			body := strconv.Itoa(i)
			mmap[i] = 0

			m, err := natsConn.Request("test", []byte(body), 10*time.Second)
			if err != nil {
				log.Fatalln(err)
				return
			}
			log.Println("[replied]", string(m.Data))

			key, err := strconv.Atoi(string(m.Data))
			if err != nil {
				log.Fatalln("invalid data", string(m.Data))
			}

			v, ok := mmap[key]
			if !ok {
				log.Fatal("unset message", m)
				return
			}

			mmap[key] = v + 1
			log.Println("set replied message", string(m.Data), v)
		}(i)
		log.Println(i)
	}

	// wait for reply
	<-time.After(time.Second)
	natsConn.FlushTimeout(time.Second)

	// check reply message
	noreplycnt := 0
	for _, v := range mmap {
		if v == 0 {
			noreplycnt++
			continue
		}
		if v > 1 {
			log.Printf("%d times reply\n", v)
		}
	}

	if noreplycnt > 0 {
		t.Errorf("%d messages are lost", noreplycnt)
	}
}
