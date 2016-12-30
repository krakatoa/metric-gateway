package main

import (
  "flag"
  "log"

  //"github.com/zorkian/go-datadog-api"
  //"github.com/oliveagle/boltq"
  //"strconv"
)

func main() {
  var (
    flagHost          = flag.String("host", "localhost", "Listen host")
    flagPort          = flag.Int("port", 5555, "Listen port")
    //flagDatadogApiKey = flag.String("datadog-api-key", "", "Datadog Api Key")
    // flagDatadogPrefix = flag.String("datadog-prefix", "riemann.", "Datadog metric prefix")
  )
  flag.Parse()

  //q, err := boltq.NewBoltQ("test_q.queue", 2000, boltq.POP_ON_FULL)
  //defer q.Close()

  //for {
  //  value, _ := q.Dequeue()
  //  if value != nil {
  //    log.Printf("value: %s", value)
  //  }
  //}

  //i := 0
  //for {
  //  i += 1
  //  err = q.Enqueue([]byte("value-" + strconv.Itoa(i)))
  //  if err != nil {
  //    log.Printf("err: %v", err)
  //  } else {
  //    log.Printf("stored: %d", i)
  //  }
  //}

  //dClient := datadog.NewClient(*flagDatadogApiKey, "riemann")

  var riemannTcp *RiemannTcp = NewRiemannTcp(*flagHost, *flagPort, func(metric BaseMetric) {
    log.Printf("Recv metric: %v", metric)
  })

  go riemannTcp.Listen()

  select {}
}

