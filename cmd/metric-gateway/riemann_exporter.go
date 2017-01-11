package main

import (
  "log"
  "fmt"
  "math"

	"github.com/amir/raidman"
  "github.com/oliveagle/boltq"
  "encoding/gob"
  "bytes"
  "encoding/base64"

  "time"
)

type RiemannExporter struct {
  ticker          *time.Ticker
  queue           *boltq.BoltQ
  client          *RiemannClient
  flushBatchSize  float64
  flushRate       float64
  queueMaxSize    int
}

func NewRiemannExporter(host string, port int, flushRate int, flushBatchSize int, queueMaxSize int) *RiemannExporter {
  q, err := boltq.NewBoltQ("riemann.queue", int64(queueMaxSize), boltq.POP_ON_FULL)
  if err != nil {
    panic("error opening bolq queue")
  }

  var riemannClient *RiemannClient = NewRiemannClient(host, port)

  return &RiemannExporter{
    ticker:         time.NewTicker(time.Duration(flushRate) * time.Second),
    queue:          q,
    client:         riemannClient,
    flushBatchSize: float64(flushBatchSize),
    flushRate:      float64(flushRate),
    queueMaxSize:   queueMaxSize,
  }
}

func (r *RiemannExporter) ConfigString() string {
  return fmt.Sprintf("riemann.config: queueName=%s, flushRate=%d, flushBatchSize=%d, queueMaxSize=%d", "riemann.queue", int(r.flushRate), int(r.flushBatchSize), r.queueMaxSize)
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

    log.Printf("event=write riemann.queue_size=%d", r.queue.Size())
  }
}

func (r *RiemannExporter) Consume() {
  for _ = range r.ticker.C {
    queueSize := r.queue.Size()
    log.Printf("Riemann Consume queueSize: %d", queueSize)
    eventsToExport := make([]BaseMetric, 0)

    min := int(math.Min(float64(queueSize), r.flushBatchSize))
    backupValues   := make([][]byte, 0)
    if queueSize > 0 {
      for i := 1; i <= min; i++ {
        value, _ := r.queue.Pop()
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
      log.Printf("event=export riemann.batch_size=%d", len(eventsToExport))

      if err := r.exportMetrics(eventsToExport); err != nil {
        log.Printf("Error trying to send Riemann batch! %s", err.Error())
        for _, backupValue := range backupValues {
          r.queue.Enqueue(backupValue)
        }
        log.Printf("Riemann Consumer wait... queueSize: %d", queueSize)
        time.Sleep(time.Duration(5) * time.Second)
      }
    }
  }
}

func (r *RiemannExporter) exportMetrics(metrics []BaseMetric) error {
  var riemannEvents []*raidman.Event = make([]*raidman.Event, 0)
  for _, metric := range metrics {
    riemannEvent := ToRiemann(metric)
    riemannEvents = append(riemannEvents, &riemannEvent)
  }

  if err := r.client.SendMulti(riemannEvents); err != nil {
    return err
  }
  return nil
}

func ToRiemann(metric BaseMetric) raidman.Event {
	var event = raidman.Event{
		State:      "ok",
		Service:    "prefix." + metric.Metric,
		Metric:     metric.Measure,
    Host:       metric.Host,
		//Attributes: [], //baseEvent.Attributes,
		Ttl:        60,
	}

  return event
}
