package main

import (
  "log"

  "github.com/oliveagle/boltq"
  "encoding/gob"
  "bytes"
  "encoding/base64"

  "time"
//  "github.com/amir/raidman"
)

type RiemannExporter struct {
  ticker        *time.Ticker
  queue         *boltq.BoltQ
}

func NewRiemannExporter() *RiemannExporter {
  q, err := boltq.NewBoltQ("riemann.queue", 3000, boltq.POP_ON_FULL)
  if err != nil {
    panic("error opening bolq queue")
  }

  return &RiemannExporter{
    ticker:       time.NewTicker(time.Duration(1000) * time.Millisecond),
    queue:        q,
  }
}

func (r *RiemannExporter) Close() {
  r.queue.Close()
}

func (r *RiemannExporter) Write(metric BaseMetric) {
  var bin bytes.Buffer
  enc := gob.NewEncoder(&bin)
  err := enc.Encode(metric)
  if err != nil {
    log.Fatal("encode error:", err)
  } else {
    base64Text := make([]byte, base64.StdEncoding.EncodedLen(len(bin.Bytes())))
    base64.StdEncoding.Encode(base64Text, bin.Bytes())
    err = r.queue.Enqueue(base64Text)
    log.Printf("Written: %v", base64Text)

    log.Printf("Riemann QUEUE SIZE: %d", r.queue.Size())
  }
}

func (r *RiemannExporter) Consume() {
  for _ = range r.ticker.C {
    for i := 1; i <= 30; i++ {
      value, _ := r.queue.Dequeue()
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
          r.exportMetric(metric)
        }
      } else {
        break
      }
    }
    log.Printf("QUEUE SIZE: %d", r.queue.Size())
  }
}

// dClient := datadog.NewClient(*flagDatadogApiKey, "riemann")
func (r *RiemannExporter) exportMetric(metric BaseMetric) {
  log.Printf("Deserialized: %v", metric)
  //var datadogMetric datadog.Metric = ToDatadog(metric, *flagDatadogPrefix)

  //metrics := make([]datadog.Metric, 1)
  //metrics = append(metrics, datadogMetric)
  //log.Printf("To Datadog: %v", metrics)

//  dClient.PostMetrics(metrics)
}
