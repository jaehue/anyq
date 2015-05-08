package qlib

import (
	"fmt"
	"reflect"
	"testing"
)

func setupNsq(q interface{}) interface{} {
	fmt.Println(reflect.TypeOf(q))
	v, ok := q.(Nsq)
	if ok {
		fmt.Println("return")
		return nil
	}

	nsq := v

	nsq.Url = "192.168.81.43:4150"
	nsq.Topic = "test"
	nsq.Channel = "qlib_test"
	fmt.Println("nsq", nsq)
	return &nsq
}

func setupNsq2(q *Nsq) {
	q.Url = "192.168.81.43:4150"
	q.Topic = "test"
	q.Channel = "qlib_test"
}

func TestConsume(t *testing.T) {
	fmt.Println("start consume test")

	q := NewNsqWithOption(setupNsq2)
	// q := &Nsq{}
	// q.Setup(setupNsq2)
	//q := NewNsq(setupNsq)

	recvCh := make(chan []byte, 100)
	err := q.BindRecvChan(recvCh)
	if err != nil {
		t.Error(err)
	}
	for m := range recvCh {
		fmt.Println(string(m))
	}

}

func BenchmarkProduce(b *testing.B) {
	fmt.Println("start benchmark test")
	b.StopTimer()

	q := NewNsq(setupNsq)
	sendCh := make(chan []byte, 100)
	err := q.BindSendChan(sendCh)
	if err != nil {
		b.Error(err)
	}

	msg := `{\"_id\" : ObjectId(\"547edbb33762370023092700\"), \"st\" : 9, \"sd\" : {\"id\" : \"rabspw\", \"type\" : 1, \"device\" : 4 }, \"ct\" : {\"text\" : \"mix: https://twitter.com/CaFitneeWorkout/status/540079252385693696\", \"document_info_ids\" : [\"N_nIpw\"] }, \"c_at\" : ISODate(\"2014-12-03T09:45:23.851Z\"), \"cn_i\" : \"91zXHg\", \"u_at\" : ISODate(\"2014-12-03T09:45:25.653Z\") }`

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		body := fmt.Sprintf("[%d]%s", i, msg)
		fmt.Println(body)
		sendCh <- []byte(body)
	}
}
