package metrics

import (
	"context"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MetricsServer struct {
	listenAddr string
	e          *echo.Echo
}

func NewMetricsServer(listenAddr string) *MetricsServer {
	e := echo.New()
	e.HideBanner = true
	e.GET("/metrics", echo.WrapHandler(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{EnableOpenMetrics: true})))

	return &MetricsServer{
		e:          e,
		listenAddr: listenAddr,
	}
}

func (s *MetricsServer) Start() {
	go func() {
		s.e.Start(s.listenAddr) // TODO: log listener errors?
	}()
}

func (s *MetricsServer) Stop() {
	s.e.Shutdown(context.TODO())
}
