package com.github.mlnick.glintfm

/**
 * Config for testing
 */
object Config {

  val config =
    """
      |glint.master.host   = "127.0.0.1"
      |glint.master.port   = 13370
      |glint {
      |  server.akka.loglevel = "INFO"
      |  server.akka.stdout-loglevel = "INFO"
      |  client.akka.loglevel = "INFO"
      |  client.akka.stdout-loglevel = "INFO"
      |  master.akka.loglevel = "INFO"
      |  master.akka.stdout-loglevel = "INFO"
      |  master.akka.remote.log-remote-lifecycle-events = on
      |  server.akka.remote.log-remote-lifecycle-events = off
      |  client.akka.remote.log-remote-lifecycle-events = on
      |  client.timeout = 30 s
      |
      |  master.akka.remote.transport-failure-detector.acceptable-heartbeat-pause = 120 s
      |  server.akka.remote.transport-failure-detector.acceptable-heartbeat-pause = 120 s
      |  client.akka.remote.transport-failure-detector.acceptable-heartbeat-pause = 120 s
      |  master.akka.remote.watch-failure-detector.acceptable-heartbeat-pause = 120 s
      |  server.akka.remote.watch-failure-detector.acceptable-heartbeat-pause = 120 s
      |  client.akka.remote.watch-failure-detector.acceptable-heartbeat-pause = 120 s
      |
      |  server.akka.remote.netty.tcp.maximum-frame-size = 32m
      |  client.akka.remote.netty.tcp.maximum-frame-size = 32m
      |  server.akka.remote.netty.tcp.send-buffer-size = 32m
      |  client.akka.remote.netty.tcp.send-buffer-size = 32m
      |  server.akka.remote.netty.tcp.receive-buffer-size = 32m
      |  client.akka.remote.netty.tcp.receive-buffer-size = 32m
      |}
    """.stripMargin
}
