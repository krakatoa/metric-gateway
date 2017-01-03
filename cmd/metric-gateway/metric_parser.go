package main

import (
  "github.com/zorkian/go-datadog-api"
)

func ToDatadog(metric BaseMetric, datadogPrefix string) datadog.Metric {
  points := make([]datadog.DataPoint, 1)
  points = append(points, datadog.DataPoint{metric.Time, metric.Measure})

  return datadog.Metric{
    Metric: datadogPrefix + metric.Metric,
    Points: points,
    Host: metric.Host,
  }
}
