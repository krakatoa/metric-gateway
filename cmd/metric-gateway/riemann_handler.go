package main

import (
  "log"

  "strings"

  //"github.com/zorkian/go-datadog-api"
  "github.com/krakatoa/metric-gateway/riemann_proto"
  "github.com/golang/protobuf/proto"
)

type BaseMetric struct {
  Metric  string
  Measure float64
  Host    string
  Time    int64
}

func ParseRiemann(binary []byte) BaseMetric {
  msg := &riemann_proto.Msg{}
  if err := proto.Unmarshal(binary, msg); err != nil {
    log.Printf("error deserializing protobuf: %s", err.Error())
  }

  var service string
  service = "prefix." + strings.Replace(strings.Replace(strings.Replace(msg.Events[0].GetService(), "/", "_", -1), " ", ".", -1), "|", "_", -1)

  var measure float64
  if msg.Events[0].MetricSint64 != nil {
    measure = float64(msg.Events[0].GetMetricSint64())
  } else if msg.Events[0].MetricF != nil {
    measure = float64(msg.Events[0].GetMetricF())
  } else if msg.Events[0].MetricD != nil {
    measure = float64(msg.Events[0].GetMetricD())
  }

  return BaseMetric{
    Metric: service,
    Measure: measure,
    Host: msg.Events[0].GetHost(),
    Time: msg.Events[0].GetTime(),
  }
}

// func RiemannTcpReader(conn net.Conn, dClient *datadog.Client, datadogPrefix *string) {
//   defer conn.Close()
//   for {
//     buf := make([]byte, 1024)
// 
//     _, err := conn.Read(buf)
//     if err != nil {
//       log.Printf("error reading: ", err.Error())
//       conn.Close()
//       return
//     } else {
//       // log.Printf("received: %v", buf)
// 
//       var nn int32
//       reader := bytes.NewReader(buf[:4])
//       err = binary.Read(reader, binary.BigEndian, &nn)
//       if err != nil {
//         log.Printf("binary read failed")
//       }
//       // log.Printf("length: %d", nn)
// 
//       // log.Printf("received buf: %v", buf[4:4 + nn])
//       var datadogMetric datadog.Metric
//       datadogMetric = ParseRiemannToDatadog(buf[4:nn + 4], datadogPrefix)
//       metrics := make([]datadog.Metric, 1)
//       metrics = append(metrics, datadogMetric)
// 
//       //log.Printf("datadog metric: %v", datadogMetric)
//       dClient.PostMetrics(metrics)
// 
//       response, err := proto.Marshal(&riemann_proto.Msg{Ok: proto.Bool(true)})
//       if err != nil {
//         log.Printf("error serializing response")
//       }
// 
//       bs := make([]byte, 4)
//       binary.BigEndian.PutUint32(bs, uint32(len(response)))
// 
//       conn.Write(append(bs, response...))
//     }
//   }
// }
