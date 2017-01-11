package main

import (
  "net"
  "bytes"
  "encoding/binary"

  "log"
  "strconv"

  "github.com/krakatoa/metric-gateway/riemann_proto"
  "github.com/golang/protobuf/proto"
)

var tcpOkResponseBytes []byte = tcpOkResponse()

type RiemannTcp struct {
  Host          string
  Port          int
  handler       func([]BaseMetric)
  listenAddress string
}

func NewRiemannTcp(host string, port int, handler func([]BaseMetric)) *RiemannTcp {
  var listenAddress string
  listenAddress = host + ":" + strconv.Itoa(port)

  return &RiemannTcp{
    Host:           host,
    Port:           port,
    handler:        handler,
    listenAddress:  listenAddress,
  }
}

func (r *RiemannTcp) Listen() {
  l, err := net.Listen("tcp", r.listenAddress)
  if err != nil {
    log.Printf("error listening: ", err.Error())
    panic("error listening")
  }
  defer l.Close()

  log.Printf("listening TCP on %s",r. listenAddress)

  for {
    conn, err := l.Accept()
    if err != nil {
      log.Printf("error accepting: ", err.Error())
      panic("error accepting")
    }

    go r.connectionHandler(conn)
  }
}

func (r *RiemannTcp) connectionHandler(conn net.Conn) {
  defer conn.Close()
  for {
    buf := make([]byte, 4096)

    _, err := conn.Read(buf)
    if err != nil {
      if err.Error() != "EOF" {
        log.Printf("error reading: ", err.Error())
        conn.Close()
        return
      }
    } else {
      metrics := ParseRiemann(protobufPayload(buf))
      // log.Printf("metrics: %v", metrics)
      r.handler(metrics)
      conn.Write(tcpOkResponseBytes)
    }
  }
}

func protobufPayload(buf []byte) []byte {
  var length int32

  reader := bytes.NewReader(buf[:4])
  err := binary.Read(reader, binary.BigEndian, &length)
  if err != nil {
    log.Printf("binary read failed")
  }

  return buf[4:length + 4]
}

func protobufOkResponse() []byte {
  response, err := proto.Marshal(&riemann_proto.Msg{Ok: proto.Bool(true)})
  if err != nil {
    log.Printf("error serializing response")
  }
  return response
}

func tcpOkResponse() []byte {
  okResponse := protobufOkResponse()

  bs := make([]byte, 4)
  binary.BigEndian.PutUint32(bs, uint32(len(okResponse)))

  return append(bs, okResponse...)
}
