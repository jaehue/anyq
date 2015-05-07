package qlib

import (
	"fmt"
	"testing"
)

func TestConsume(t *testing.T) {
	q := NewNsq("192.168.81.43:4150")
	recvCh := make(chan []byte, 100)
	err := q.BindRecvChan("test", "qlib", recvCh)
	if err != nil {
		t.Error(err)
	}
	for m := range recvCh {
		fmt.Println(string(m))
	}

}

func BenchmarkProduce(b *testing.B) {
	b.StopTimer()

	q := NewNsq("192.168.81.43:4150")
	sendCh := make(chan []byte, 100)
	err := q.BindSendChan("test", sendCh)
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
