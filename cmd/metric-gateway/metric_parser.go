package main

import (
  "log"
  "github.com/krakatoa/metric-gateway/riemann_proto"
  "github.com/golang/protobuf/proto"
  "github.com/zorkian/go-datadog-api"
  "strings"
)

func ParseRiemannToDatadog(binary []byte, datadogPrefix *string) datadog.Metric {
  msg := &riemann_proto.Msg{}
  if err := proto.Unmarshal(binary, msg); err != nil {
    log.Printf("error deserializing protobuf: %s", err.Error())
  }
  //log.Printf("deserialized: %v", msg)

  var service string
  service = *datadogPrefix + strings.Replace(strings.Replace(strings.Replace(msg.Events[0].GetService(), "/", "_", -1), " ", ".", -1), "|", "_", -1)

  var metric float64
  if msg.Events[0].MetricSint64 != nil {
    metric = float64(msg.Events[0].GetMetricSint64())
  } else if msg.Events[0].MetricF != nil {
    metric = float64(msg.Events[0].GetMetricF())
  } else if msg.Events[0].MetricD != nil {
    metric = float64(msg.Events[0].GetMetricD())
  }

  points := make([]datadog.DataPoint, 1)
  points = append(points, datadog.DataPoint{float64(msg.Events[0].GetTime()), metric})

  return datadog.Metric{
    Metric: service,
    Points: points,
    Host: msg.Events[0].GetHost(),
  }
}
