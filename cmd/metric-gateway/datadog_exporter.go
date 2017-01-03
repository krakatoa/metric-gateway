package main

import (
  "log"

  "github.com/oliveagle/boltq"
  "encoding/gob"
  "bytes"
  "encoding/base64"
  //"github.com/zorkian/go-datadog-api"
)

type DatadogExporter struct {
  queue         *boltq.BoltQ
}

func NewDatadogExporter() *DatadogExporter {
  q, err := boltq.NewBoltQ("datadog.queue", 2000, boltq.POP_ON_FULL)
  if err != nil {
    panic("error opening bolq queue")
  }

  return &DatadogExporter{
    queue:        q,
  }
}

func (d *DatadogExporter) Close() {
  d.queue.Close()
}

func (d *DatadogExporter) Write(metric BaseMetric) {
  var bin bytes.Buffer
  enc := gob.NewEncoder(&bin)
  err := enc.Encode(metric)
  if err != nil {
    log.Fatal("encode error:", err)
  } else {
    base64Text := make([]byte, base64.StdEncoding.EncodedLen(len(bin.Bytes())))
    base64.StdEncoding.Encode(base64Text, bin.Bytes())
    err = d.queue.Enqueue(base64Text)
    log.Printf("Written: %v", base64Text)
  }
}

func (d *DatadogExporter) Consume() {
  for {
    value, _ := d.queue.Dequeue()
    if value != nil {
      base64Text := make([]byte, base64.StdEncoding.DecodedLen(len(value)))
      base64.StdEncoding.Decode(base64Text, value)

      var metric BaseMetric
      var binReader *bytes.Reader = bytes.NewReader(base64Text)
      dec := gob.NewDecoder(binReader)

      err := dec.Decode(&metric)
      if err != nil {
        log.Fatal("decode error:", err)
      } else {
        d.exportMetric(metric)
      }
    }
  }
}

// dClient := datadog.NewClient(*flagDatadogApiKey, "riemann")
func (d *DatadogExporter) exportMetric(metric BaseMetric) {
  log.Printf("Deserialized: %v", metric)
  //var datadogMetric datadog.Metric = ToDatadog(metric, *flagDatadogPrefix)

  //metrics := make([]datadog.Metric, 1)
  //metrics = append(metrics, datadogMetric)
  //log.Printf("To Datadog: %v", metrics)

//  dClient.PostMetrics(metrics)
}
