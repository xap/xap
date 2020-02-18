job "zipkin" {
  datacenters = ["dc1"]

  group "zipkin" {
    task "zipkin" {
      driver = "docker"

      config {
        image = "openzipkin/zipkin"
        port_map {
          http = 9411
        }
      }

      resources {
        network {
          mbits = 10
          port "http" {
            static = "9411"
          }
        }
      }
      service {
        name = "zipkin"
        port = "http"
        check {
          name = "zipkin"
          type = "http"
          path = "/"
          interval = "1s"
          timeout = "5s"
        }
      }
    }
  }
}