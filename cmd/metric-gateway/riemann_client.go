package main

import (
	"log"

	"github.com/amir/raidman"
  "strconv"
)

type RiemannClient struct {
	host              string
	port              int
	client            *raidman.Client
}

func NewRiemannClient(riemannHost string, riemannPort int) *RiemannClient {
	riemann := &RiemannClient{
		host:              riemannHost,
		port:              riemannPort,
	}
	riemann.startClient()

	return riemann
}

func (r *RiemannClient) startClient() error {
  var clientAddress string
  clientAddress = r.host + ":" + strconv.Itoa(r.port)

	if client, err := raidman.Dial("tcp", clientAddress); err != nil {
    return err
		// log.Println("error starting Riemann client")
	} else {
		r.client = client
    return nil
	}
}

func (r *RiemannClient) SendMulti(events []*raidman.Event) error {
  if r.client == nil {
    if err := r.startClient(); err != nil {
      return err
    }
  }

	log.Printf("Events: %v", events)
	err := r.client.SendMulti(events)
	if err != nil {
		// log.Println("error sending Riemann event: %s", err)
		r.client.Close()
		r.client = nil
    return err
	}
  return nil
}
