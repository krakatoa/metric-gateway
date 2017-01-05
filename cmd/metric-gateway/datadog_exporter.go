package main

import (
  "log"

  "github.com/oliveagle/boltq"
  "encoding/gob"
  "bytes"
  "encoding/base64"

  "time"
  "github.com/zorkian/go-datadog-api"
  "math"
  "fmt"
)

type DatadogExporter struct {
  ticker          *time.Ticker
  queue           *boltq.BoltQ
  client          *datadog.Client
  prefix          string
  flushBatchSize  float64
  flushRate       float64
  queueMaxSize    int
}

func NewDatadogExporter(apiKey string, appKey string, prefix string, flushRate int, flushBatchSize int, queueMaxSize int) *DatadogExporter {
  q, err := boltq.NewBoltQ("datadog.queue", int64(queueMaxSize), boltq.POP_ON_FULL)
  if err != nil {
    panic("error opening bolq queue")
  }

  datadogClient := datadog.NewClient(apiKey, appKey)

  return &DatadogExporter{
    ticker:         time.NewTicker(time.Duration(flushRate) * time.Second),
    queue:          q,
    client:         datadogClient,
    prefix:         prefix,
    flushBatchSize: float64(flushBatchSize),
    flushRate:      float64(flushRate),
    queueMaxSize:   queueMaxSize,
  }
}

func (d *DatadogExporter) ConfigString() string {
  return fmt.Sprintf("datadog.config: queueName=%s, flushRate=%d, flushBatchSize=%d, queueMaxSize=%d, prefix=%s", "datadog.queue", int(d.flushRate), int(d.flushBatchSize), d.queueMaxSize, d.prefix)
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

    log.Printf("event=write datadog.queue_size=%d", d.queue.Size())
  }
}

func (d *DatadogExporter) Consume() {
  for _ = range d.ticker.C {
    queueSize := d.queue.Size()
    log.Printf("Datadog Consume queueSize: %d", queueSize)
    eventsToExport := make([]BaseMetric, 0)

    min := int(math.Min(float64(queueSize), d.flushBatchSize))
    backupValues   := make([][]byte, 0)
    if queueSize > 0 {
      for i := 1; i <= min; i++ {
        value, _ := d.queue.Pop()
        if value != nil {
          tmp := make([]byte, len(value))
          copy(tmp, value)
          backupValues = append(backupValues, tmp)

          base64Text := make([]byte, base64.StdEncoding.DecodedLen(len(value)))
          base64.StdEncoding.Decode(base64Text, value)

          var metric BaseMetric
          var binReader *bytes.Reader = bytes.NewReader(base64Text)
          dec := gob.NewDecoder(binReader)

          err := dec.Decode(&metric)
          if err != nil {
            log.Printf("decode error:", err)
          } else {
            eventsToExport = append(eventsToExport, metric)
          }
        } else {
          break
        }
      }
      log.Printf("event=export datadog.batch_size=%d", len(eventsToExport))

      if err := d.exportMetrics(eventsToExport); err != nil {
        log.Printf("Error trying to send Datadog batch! %s", err.Error())
        for _, backupValue := range backupValues {
          d.queue.Enqueue(backupValue)
        }
        log.Printf("Datadog Consumer wait... queueSize: %d", queueSize)
        time.Sleep(time.Duration(5) * time.Second)
      }
    }
  }
}

func (d *DatadogExporter) exportMetrics(metrics []BaseMetric) error {
  var datadogMetrics []datadog.Metric = make([]datadog.Metric, 0) // 1
  for _, metric := range metrics {
    datadogMetric := ToDatadog(metric, d.prefix)
    datadogMetrics = append(datadogMetrics, datadogMetric)
  }

  log.Printf("To Datadog batch: %v", datadogMetrics)
  return d.client.PostMetrics(datadogMetrics)
}

func ToDatadog(metric BaseMetric, datadogPrefix string) datadog.Metric {
  points := make([]datadog.DataPoint, 0)
  points = append(points, datadog.DataPoint{metric.Time, metric.Measure})

  return datadog.Metric{
    Metric: datadogPrefix + metric.Metric,
    Points: points,
    Host: metric.Host,
  }
}
