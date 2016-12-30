package main

import (
  "net"

  "log"
  "strconv"
)

type RiemannUdp struct {
  Host          string
  Port          int
  handler       func(BaseMetric)
  listenAddress *net.UDPAddr
}

func NewRiemannUdp(host string, port int, handler func(BaseMetric)) *RiemannUdp {
  var listenAddress string
  listenAddress = host + ":" + strconv.Itoa(port)

  addr, err := net.ResolveUDPAddr("udp", listenAddress)
  if err != nil {
    panic("err listening udp")
  }

  return &RiemannUdp{
    Host:           host,
    Port:           port,
    handler:        handler,
    listenAddress:  addr,
  }
}

func (r *RiemannUdp) Listen() {
  l, err := net.ListenUDP("udp", r.listenAddress)
  if err != nil {
    log.Printf("error listening: ", err.Error())
    panic("error listening")
  }

  log.Printf("listening UDP on %s",r. listenAddress)

  go r.connectionHandler(l)
}

func (r *RiemannUdp) connectionHandler(conn *net.UDPConn) {
  defer conn.Close()

  buf := make([]byte, 1024)

  for {
    //_, err := conn.Read(buf)
    n, _, err := conn.ReadFromUDP(buf)

    if err != nil {
      log.Printf("err: %s", err.Error())
    } else {
      metric := ParseRiemann(buf[0:n])
      r.handler(metric)
    }

  }
}
