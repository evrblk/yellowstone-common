package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MetricsServer struct {
	server *http.Server
}

func NewMetricsServer(listenAddr string) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{EnableOpenMetrics: true}))

	return &MetricsServer{
		server: &http.Server{
			Addr:    listenAddr,
			Handler: mux,
		},
	}
}

func (s *MetricsServer) Start() {
	go s.server.ListenAndServe() // TODO: log listener errors?
}

func (s *MetricsServer) Stop() {
	s.server.Shutdown(context.TODO())
}
