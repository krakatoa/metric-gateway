package main

import (
  "flag"
//  "log"
)

func main() {
  var (
    flagHost          = flag.String("host", "localhost", "Listen host")
    flagPort          = flag.Int("port", 5555, "Listen port")
    //flagDatadogApiKey = flag.String("datadog-api-key", "", "Datadog Api Key")
    //flagDatadogPrefix = flag.String("datadog-prefix", "riemann.", "Datadog metric prefix")
  )
  flag.Parse()

  var datadogExporter *DatadogExporter = NewDatadogExporter()
  var riemannExporter *RiemannExporter = NewRiemannExporter()

  defer datadogExporter.Close()
  go datadogExporter.Consume()

  defer riemannExporter.Close()
  go riemannExporter.Consume()

  var riemannTcp *RiemannTcp = NewRiemannTcp(*flagHost, *flagPort, func(metric BaseMetric) {
    // log.Printf("TCP Recv metric: %v", metric)
    datadogExporter.Write(metric)
    riemannExporter.Write(metric)
  })

  go riemannTcp.Listen()

  var riemannUdp *RiemannUdp = NewRiemannUdp(*flagHost, *flagPort, func(metric BaseMetric) {
    // log.Printf("UDP Recv metric: %v", metric)
    datadogExporter.Write(metric)
    riemannExporter.Write(metric)
  })

  go riemannUdp.Listen()

  select {}
}
