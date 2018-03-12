package main

import (
	"database/sql"
	"flag"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-oci8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

var (
	// La version sera définie au build time.
	Version       = "0.0.1.dev"
	listenAddress = flag.String("web.listen-address", ":9161", "Address to listen on for web interface and telemetry.")
	metricPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	landingPage   = []byte("<html><head><title>Oracle DB Exporter " + Version + "</title></head><body><h1>Oracle DB Exporter " + Version + "</h1><p><a href='" + *metricPath + "'>Metrics</a></p></body></html>")
)

// Nom racine des métriques.
const (
	namespace = "oracledb"
	exporter  = "exporter"
)

// L'exporter collecte les metriques. Il implémente prometheus.Collector.
type Exporter struct {
	dsn             string
	duration, error prometheus.Gauge
	totalScrapes    prometheus.Counter
	scrapeErrors    *prometheus.CounterVec
	up              prometheus.Gauge
}

// NewExporter retourne un nouveau exporter Oracle DB pour les DSN fournis.
func NewExporter(dsn string) *Exporter {
	return &Exporter{
		dsn: dsn,
		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_duration_seconds",
			Help:      "Duration of the last scrape of metrics from Oracle DB.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrapes_total",
			Help:      "Total number of times Oracle DB was scraped for metrics.",
		}),
		scrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occured scraping a Oracle database.",
		}, []string{"collector"}),
		error: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from Oracle DB resulted in an error (1 for error, 0 for success).",
		}),
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the Oracle database server is up.",
		}),
	}
}

// Describe describes all the metrics exported by the MS SQL exporter.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics. The problem
	// here is that we need to connect to the Oracle DB. If it is currently
	// unavailable, the descriptors will be incomplete. Since this is a
	// stand-alone exporter and not used as a library within other code
	// implementing additional metrics, the worst that can happen is that we
	// don't detect inconsistent metrics created by this exporter
	// itself. Also, a change in the monitored Oracle instance may change the
	// exported metrics during the runtime of the exporter.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh

}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.scrape(ch)
	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.error
	e.scrapeErrors.Collect(ch)
	ch <- e.up
}

func (e *Exporter) scrape(ch chan<- prometheus.Metric) {
	e.totalScrapes.Inc()
	var err error
	defer func(begun time.Time) {
		e.duration.Set(time.Since(begun).Seconds())
		if err == nil {
			e.error.Set(0)
		} else {
			e.error.Set(1)
		}
	}(time.Now())

	db, err := sql.Open("oci8", e.dsn)
	if err != nil {
		log.Errorln("Error opening connection to database:", err)
		return
	}
	defer db.Close()

	isUpRows, err := db.Query("SELECT 1 FROM DUAL")
	if err != nil {
		log.Errorln("Error pinging oracle:", err)
		e.up.Set(0)
		return
	}
	isUpRows.Close()
	e.up.Set(1)

	if err = ScrapeSessions(db, ch); err != nil {
		log.Errorln("Error scraping for sessions:", err)
		e.scrapeErrors.WithLabelValues("sessions").Inc()
	}

	if err = ScrapeActivity(db, ch); err != nil {
		log.Errorln("Error scraping for activity:", err)
		e.scrapeErrors.WithLabelValues("activity").Inc()
	}

	if err = ScrapeBaseStatus(db, ch); err != nil {
		log.Errorln("Error scraping for status:", err)
		e.scrapeErrors.WithLabelValues("status").Inc()
	}

	if err = ScrapeFRA(db, ch); err != nil {
		log.Errorln("Error scraping for fra:", err)
		e.scrapeErrors.WithLabelValues("fra").Inc()
	}

	if err = ScrapeTablespace(db, ch); err != nil {
		log.Errorln("Error scraping for tablespace:", err)
		e.scrapeErrors.WithLabelValues("tablespace").Inc()
	}

	if err = ScrapeUsers(db, ch); err != nil {
		log.Errorln("Error scraping for users:", err)
		e.scrapeErrors.WithLabelValues("users").Inc()
	}

	if err = ScrapeRMAN(db, ch); err != nil {
		log.Errorln("Error scraping for rman:", err)
		e.scrapeErrors.WithLabelValues("rman").Inc()
	}	


}

// ScrapeSessions collects session metrics from the v$session view.
func ScrapeSessions(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	// Retrieve status and type for all sessions.
	rows, err = db.Query("SELECT status, type, COUNT(*) FROM v$session GROUP BY status, type")
	if err != nil {
		return err
	}

	defer rows.Close()
	activeCount := 0.
	inactiveCount := 0.
	for rows.Next() {
		var (
			status      string
			sessionType string
			count       float64
		)
		if err := rows.Scan(&status, &sessionType, &count); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "sessions", "activity"),
				"Gauge metric with count of sessions by status and type", []string{"status", "type"}, nil),
			prometheus.GaugeValue,
			count,
			status,
			sessionType,
		)

		// These metrics are deprecated though so as to not break existing monitoring straight away, are included for the next few releases.
		if status == "ACTIVE" {
			activeCount += count
		}

		if status == "INACTIVE" {
			inactiveCount += count
		}
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(prometheus.BuildFQName(namespace, "sessions", "active"),
			"Gauge metric with count of sessions marked ACTIVE. DEPRECATED: use sum(oracledb_sessions_activity{status='ACTIVE}) instead.", []string{}, nil),
		prometheus.GaugeValue,
		activeCount,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(prometheus.BuildFQName(namespace, "sessions", "inactive"),
			"Gauge metric with count of sessions marked INACTIVE. DEPRECATED: use sum(oracledb_sessions_activity{status='INACTIVE'}) instead.", []string{}, nil),
		prometheus.GaugeValue,
		inactiveCount,
	)
	return nil
}

// ScrapeActivity collects activity metrics from the v$sysstat view.
func ScrapeActivity(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT name, value FROM v$sysstat WHERE name IN ('parse count (total)', 'execute count', 'user commits', 'user rollbacks')")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var value float64
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		name = cleanName(name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "activity", name),
				"Generic counter metric from v$sysstat view in Oracle.", []string{}, nil),
			prometheus.CounterValue,
			value,
		)
	}
	return nil
}

// ScrapeBaseStatus collects session metrics from the v$instance view.
func ScrapeBaseStatus(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT instance_name as name, status from v$instance")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var name string
		var value float64
		if err := rows.Scan(&name, &status); err != nil {
			return err
		}
		if status == "OPEN" {
			value = 2
		} else
		if status == "MOUNTED" {
			value = 1
		} else {
		value = 0
		}
			ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "status", name),
				"Statut de la base", []string{}, nil),
				prometheus.CounterValue,
				value,
		)
	}
	return nil
}

// ScrapeTablespace collects tablespace used space and if it's autoextensible.
func ScrapeTablespace(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query(`
select distinct dba_tablespace_usage_metrics.tablespace_name, dba_data_files.autoextensible, trunc(dba_tablespace_usage_metrics.used_percent) from dba_tablespace_usage_metrics, dba_data_files
`)
	if err != nil {
		return err
	}
	defer rows.Close()

	tablespaceFreePercentDesc := prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "tablespace", "free"),
		"Generic counter metric of tablespaces free bytes in Oracle.",
		[]string{"tablespace_name", "autoextensible"}, nil,
	)

	for rows.Next() {
		var tablespace_name string
		var autoextensible string
		var percent_free float64

		if err := rows.Scan(&tablespace_name, &autoextensible, &percent_free); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(tablespaceFreePercentDesc, prometheus.GaugeValue, float64(percent_free), tablespace_name, autoextensible)
	}
	return nil
}


func ScrapeUsers(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	// Retrieve status and type for all sessions.
	rows, err = db.Query("SELECT TRUNC(expiry_date) - TRUNC(sysdate) as expiration, username, REPLACE(account_status, ' ', '') FROM dba_users")
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			expiration float64
			username string
			account_status string
		)
		if err := rows.Scan(&expiration, &username, &account_status); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "users", "expiration"),
				"Utilisateurs", []string{"username", "status"}, nil),
			prometheus.CounterValue,
			expiration,
			username,
			account_status,
		)
	}
	return nil
}

func ScrapeFRA(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)

	rows, err = db.Query("select trunc(sum(PERCENT_SPACE_USED)) as value from v$flash_recovery_area_usage")
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			value float64
		)
		if err := rows.Scan(&value); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "fra","used"),
				"FRA disponible", []string{}, nil),
				prometheus.GaugeValue,
				value,
		)
	}
	return nil
}

func ScrapeRMAN(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	// Retrieve status and type for all sessions.
	rows, err = db.Query("SELECT row_level, operation AS bkp_operation, status AS bkp_status, object_type, TO_CHAR(start_time,'YYYY-MM-DD_HH24:MI:SS') AS bkp_start_time, TO_CHAR(end_time,'YYYY-MM-DD_HH24:MI:SS') as bkp_end_time, TO_CHAR(start_time,'DD') as bkp_start_nbday FROM V$RMAN_STATUS WHERE START_TIME > SYSDATE -1 AND operation = 'BACKUP'")
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			Value float64
			row_level float64
			bkp_operation string
			bkp_status string
			object_type string
			bkp_start_time string
			bkp_end_time string
			bkp_start_nbday string
		)
		if err := rows.Scan(&row_level, &bkp_operation, &bkp_status, &object_type, &bkp_start_time, &bkp_end_time, &bkp_start_nbday); err != nil {
			return err
		}

		if bkp_status == "COMPLETED" {
			Value = 0	// Green value
		} else
		if bkp_status == "RUNNING" {
			Value = 1 // Yellow value
		} else {
		Value = 2 // Red value
		}

		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "rman", "backup"),
			"Backup RMAN", []string{"operation", "status", "type", "start_time", "end_time", "start_nbday"}, nil),
			prometheus.GaugeValue,
			Value,
			bkp_operation,
			bkp_status,
			object_type,
			bkp_start_time,
			bkp_end_time,
			bkp_start_nbday,
		)
	}
	return nil
}


// Oracle gives us some ugly names back. This function cleans things up for Prometheus.
func cleanName(s string) string {
	s = strings.Replace(s, " ", "_", -1) // Remove spaces
	s = strings.Replace(s, "(", "", -1)  // Remove open parenthesis
	s = strings.Replace(s, ")", "", -1)  // Remove close parenthesis
	s = strings.Replace(s, "/", "", -1)  // Remove forward slashes
	s = strings.ToLower(s)
	return s
}

func main() {
	flag.Parse()
	log.Infoln("Starting oracledb_exporter " + Version)
	dsn := os.Getenv("DATA_SOURCE_NAME")
	exporter := NewExporter(dsn)
	prometheus.MustRegister(exporter)
	http.Handle(*metricPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(landingPage)
	})
	log.Infoln("Listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
