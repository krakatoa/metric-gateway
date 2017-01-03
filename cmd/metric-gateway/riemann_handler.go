package main

import (
  "log"

  "strings"

  "github.com/krakatoa/metric-gateway/riemann_proto"
  "github.com/golang/protobuf/proto"
)

type BaseMetric struct {
  Metric  string
  Measure float64
  Host    string
  Time    float64
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
    Time: float64(msg.Events[0].GetTime()),
  }
}
