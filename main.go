package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ── Clash API response structures ───────────────────────────────────────

type ConnectionsResponse struct {
	DownloadTotal int64        `json:"downloadTotal"`
	UploadTotal   int64        `json:"uploadTotal"`
	Connections   []Connection `json:"connections"`
}

type Connection struct {
	ID          string   `json:"id"`
	Upload      int64    `json:"upload"`
	Download    int64    `json:"download"`
	Start       string   `json:"start"`
	Metadata    Meta     `json:"metadata"`
	Chain       []string `json:"chain"`
	Rule        string   `json:"rule"`
	RulePayload string   `json:"rulePayload"`
}

type Meta struct {
	Network    string `json:"network"`
	Type       string `json:"type"`
	SourceIP   string `json:"sourceIP"`
	DestIP     string `json:"destinationIP"`
	SourcePort string `json:"sourcePort"`
	DestPort   string `json:"destinationPort"`
	Host       string `json:"host"`
	DNSMode    string `json:"dnsMode"`
}

type MemoryResponse struct {
	InUse   int64 `json:"inuse"`
	OSLimit int64 `json:"os-limit"`
}

// ── Prometheus metrics ──────────────────────────────────────────────────

var (
	singBoxUploadTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sing_box_upload_bytes_total",
		Help: "Total upload bytes across all connections",
	})
	singBoxDownloadTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sing_box_download_bytes_total",
		Help: "Total download bytes across all connections",
	})

	singBoxConnectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sing_box_connections_active",
		Help: "Number of currently active connections",
	})

	singBoxConnectionsByNetwork = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sing_box_connections_by_network",
		Help: "Active connections grouped by network type",
	}, []string{"network"})

	singBoxConnectionsByInbound = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sing_box_connections_by_inbound",
		Help: "Active connections grouped by inbound tag",
	}, []string{"inbound"})

	singBoxConnectionsByOutbound = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sing_box_connections_by_outbound",
		Help: "Active connections grouped by outbound tag",
	}, []string{"outbound"})

	singBoxConnectionUpload = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sing_box_connection_upload_bytes",
		Help: "Upload bytes for a specific active connection",
	}, []string{"id", "network", "source", "destination", "outbound"})

	singBoxConnectionDownload = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sing_box_connection_download_bytes",
		Help: "Download bytes for a specific active connection",
	}, []string{"id", "network", "source", "destination", "outbound"})

	singBoxMemoryInUse = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sing_box_memory_inuse_bytes",
		Help: "Current memory usage by sing-box",
	})
	singBoxMemoryOSLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sing_box_memory_os_limit_bytes",
		Help: "OS memory limit for sing-box process",
	})

	singBoxScrapeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sing_box_scrape_errors_total",
		Help: "Total number of errors scraping sing-box Clash API",
	}, []string{"endpoint"})
)

// ── Exporter ────────────────────────────────────────────────────────────

type Exporter struct {
	singBoxURL string
	client     *http.Client
	mu         sync.Mutex
}

func NewExporter(singBoxURL string) *Exporter {
	return &Exporter{
		singBoxURL: singBoxURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (e *Exporter) scrape() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.scrapeConnections()
	e.scrapeMemory()
}

func (e *Exporter) scrapeConnections() {
	url := e.singBoxURL + "/connections"
	resp, err := e.client.Get(url)
	if err != nil {
		log.Printf("ERROR: failed to scrape %s: %v", url, err)
		singBoxScrapeErrors.WithLabelValues("connections").Inc()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("ERROR: %s returned status %d", url, resp.StatusCode)
		singBoxScrapeErrors.WithLabelValues("connections").Inc()
		return
	}

	var data ConnectionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Printf("ERROR: failed to decode connections response: %v", err)
		singBoxScrapeErrors.WithLabelValues("connections").Inc()
		return
	}

	// Reset per-connection gauges before repopulating
	singBoxConnectionUpload.Reset()
	singBoxConnectionDownload.Reset()
	singBoxConnectionsByNetwork.Reset()
	singBoxConnectionsByInbound.Reset()
	singBoxConnectionsByOutbound.Reset()

	singBoxUploadTotal.Set(float64(data.UploadTotal))
	singBoxDownloadTotal.Set(float64(data.DownloadTotal))
	singBoxConnectionsActive.Set(float64(len(data.Connections)))

	networkCounts := make(map[string]float64)
	inboundCounts := make(map[string]float64)
	outboundCounts := make(map[string]float64)

	for _, conn := range data.Connections {
		network := conn.Metadata.Network
		if network == "" {
			network = "unknown"
		}
		networkCounts[network]++

		inbound := "direct"
		if len(conn.Chain) > 0 {
			inbound = conn.Chain[0]
		}
		inboundCounts[inbound]++

		outbound := "direct"
		if len(conn.Chain) > 1 {
			outbound = conn.Chain[len(conn.Chain)-1]
		} else if len(conn.Chain) == 1 {
			outbound = conn.Chain[0]
		}
		outboundCounts[outbound]++

		src := conn.Metadata.SourceIP + ":" + conn.Metadata.SourcePort
		dst := conn.Metadata.DestIP + ":" + conn.Metadata.DestPort
		if conn.Metadata.Host != "" {
			dst = conn.Metadata.Host + ":" + conn.Metadata.DestPort
		}

		singBoxConnectionUpload.WithLabelValues(conn.ID, network, src, dst, outbound).Set(float64(conn.Upload))
		singBoxConnectionDownload.WithLabelValues(conn.ID, network, src, dst, outbound).Set(float64(conn.Download))
	}

	for net, count := range networkCounts {
		singBoxConnectionsByNetwork.WithLabelValues(net).Set(count)
	}
	for in, count := range inboundCounts {
		singBoxConnectionsByInbound.WithLabelValues(in).Set(count)
	}
	for out, count := range outboundCounts {
		singBoxConnectionsByOutbound.WithLabelValues(out).Set(count)
	}

	log.Printf("Scraped connections: total=%d, upload=%d, download=%d",
		len(data.Connections), data.UploadTotal, data.DownloadTotal)
}

func (e *Exporter) scrapeMemory() {
	url := e.singBoxURL + "/memory"
	resp, err := e.client.Get(url)
	if err != nil {
		log.Printf("ERROR: failed to scrape %s: %v", url, err)
		singBoxScrapeErrors.WithLabelValues("memory").Inc()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("WARN: %s returned status %d (memory endpoint may not be available)", url, resp.StatusCode)
		return
	}

	var data MemoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		log.Printf("ERROR: failed to decode memory response: %v", err)
		singBoxScrapeErrors.WithLabelValues("memory").Inc()
		return
	}

	singBoxMemoryInUse.Set(float64(data.InUse))
	singBoxMemoryOSLimit.Set(float64(data.OSLimit))

	log.Printf("Scraped memory: inuse=%d, os_limit=%d", data.InUse, data.OSLimit)
}

// ── Main ────────────────────────────────────────────────────────────────

func main() {
	singBoxURL := getEnv("SING_BOX_URL", "http://tunnelium-sing-box:9090")
	listenAddr := getEnv("LISTEN_ADDR", ":9120")
	scrapeInterval := 15 * time.Second

	log.Printf("sing-box exporter starting")
	log.Printf("  sing-box API: %s", singBoxURL)
	log.Printf("  listen:       %s", listenAddr)
	log.Printf("  interval:     %s", scrapeInterval)

	prometheus.MustRegister(
		singBoxUploadTotal,
		singBoxDownloadTotal,
		singBoxConnectionsActive,
		singBoxConnectionsByNetwork,
		singBoxConnectionsByInbound,
		singBoxConnectionsByOutbound,
		singBoxConnectionUpload,
		singBoxConnectionDownload,
		singBoxMemoryInUse,
		singBoxMemoryOSLimit,
		singBoxScrapeErrors,
	)

	exporter := NewExporter(singBoxURL)

	// Initial scrape
	exporter.scrape()

	// Periodic scraping in background
	go func() {
		ticker := time.NewTicker(scrapeInterval)
		defer ticker.Stop()
		for range ticker.C {
			exporter.scrape()
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	log.Printf("listening on %s", listenAddr)
	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		log.Fatalf("FATAL: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
