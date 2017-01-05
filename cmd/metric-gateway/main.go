package main

import (
  "flag"
  "log"
)

func main() {
  var (
    flagHost            = flag.String("host", "localhost", "Listen host")
    flagPort            = flag.Int("port", 5555, "Listen port")

    flagRiemannEnabled  = flag.Bool("riemann-enabled", true, "Enable Riemann exporter")
    flagRiemannHost     = flag.String("riemann-host", "", "Riemann host")
    flagRiemannPort     = flag.Int("riemann-port", 5555, "Riemann port")
    flagRiemannFlushRate      = flag.Int("riemann-flush-rate", 1, "Riemann flush rate in seconds")
    flagRiemannFlushBatchSize = flag.Int("riemann-flush-batch-size", 30, "Riemann max messages per batch flush")
    flagRiemannMaxQueueSize = flag.Int("riemann-max-queue-size", 30000, "Riemann max queue size")

    flagDatadogEnabled  = flag.Bool("datadog-enabled", false, "Enable Datadog exporter")
    flagDatadogApiKey   = flag.String("datadog-api-key", "", "Datadog Api Key")
    flagDatadogAppKey   = flag.String("datadog-app-key", "metric-gateway", "Datadog App Key")
    flagDatadogPrefix   = flag.String("datadog-prefix", "riemann.", "Datadog metric prefix")
    flagDatadogFlushRate      = flag.Int("datadog-flush-rate", 1, "Datadog flush rate in seconds")
    flagDatadogFlushBatchSize = flag.Int("datadog-flush-batch-size", 30, "Datadog max messages per batch flush")
    flagDatadogMaxQueueSize   = flag.Int("datadog-max-queue-size", 30000, "Datadog max queue size")
  )
  flag.Parse()

  var riemannExporter *RiemannExporter
  if *flagRiemannEnabled {
    riemannExporter = NewRiemannExporter(*flagRiemannHost, *flagRiemannPort, *flagRiemannFlushRate, *flagRiemannFlushBatchSize, *flagRiemannMaxQueueSize)
    log.Printf(riemannExporter.ConfigString())
    defer riemannExporter.Close()
    go riemannExporter.Consume()
  }

  var datadogExporter *DatadogExporter
  if *flagDatadogEnabled {
    datadogExporter = NewDatadogExporter(*flagDatadogApiKey, *flagDatadogAppKey, *flagDatadogPrefix, *flagDatadogFlushRate, *flagDatadogFlushBatchSize, *flagDatadogMaxQueueSize)
    log.Printf(datadogExporter.ConfigString())
    defer datadogExporter.Close()
    go datadogExporter.Consume()
  }

  var riemannTcp *RiemannTcp = NewRiemannTcp(*flagHost, *flagPort, func(metric BaseMetric) {
    // log.Printf("TCP Recv metric: %v", metric)
    if *flagDatadogEnabled { datadogExporter.Write(metric) }
    if *flagRiemannEnabled { riemannExporter.Write(metric) }
  })
  go riemannTcp.Listen()

  var riemannUdp *RiemannUdp = NewRiemannUdp(*flagHost, *flagPort, func(metric BaseMetric) {
    // log.Printf("UDP Recv metric: %v", metric)
    if *flagDatadogEnabled { datadogExporter.Write(metric) }
    if *flagRiemannEnabled { riemannExporter.Write(metric) }
  })
  go riemannUdp.Listen()

  select {}
}
