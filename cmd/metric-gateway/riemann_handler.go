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

func ParseRiemann(binary []byte) []BaseMetric {
  msg := &riemann_proto.Msg{}
  if err := proto.Unmarshal(binary, msg); err != nil {
    log.Printf("error deserializing protobuf: %s", err.Error())
  }

  var baseMetrics []BaseMetric = make([]BaseMetric, 0)
  var service string
  var measure float64

  for _, protoEvent := range msg.Events {
    service = strings.Replace(strings.Replace(strings.Replace(protoEvent.GetService(), "/", "_", -1), " ", ".", -1), "|", "_", -1)

    if protoEvent.MetricSint64 != nil {
      measure = float64(protoEvent.GetMetricSint64())
    } else if protoEvent.MetricF != nil {
      measure = float64(protoEvent.GetMetricF())
    } else if protoEvent.MetricD != nil {
      measure = float64(protoEvent.GetMetricD())
    }
    baseMetrics = append(baseMetrics, BaseMetric{
      Metric: service,
      Measure: measure,
      Host: protoEvent.GetHost(),
      Time: float64(protoEvent.GetTime()),
    })
  }

  return baseMetrics
}
