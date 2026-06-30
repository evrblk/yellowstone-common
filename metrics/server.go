package metrics

import (
	"context"
	"fmt"
	"log"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MetricsServer struct {
	port int
	e    *echo.Echo
}

func NewMetricsServer(port int) *MetricsServer {
	e := echo.New()
	e.HideBanner = true
	e.GET("/metrics", echo.WrapHandler(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{EnableOpenMetrics: true})))

	return &MetricsServer{
		e:    e,
		port: port,
	}
}

func (s *MetricsServer) Start() {
	go func() {
		s.e.Start(fmt.Sprintf(":%d", s.port))
	}()
}

func (s *MetricsServer) Stop() {
	log.Printf("Stopping Metrics Server")

	s.e.Shutdown(context.TODO())
}
