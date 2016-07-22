package logdna

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"text/template"
	"time"

	"github.com/gliderlabs/logspout/router"
)

const API_URL = "https://logs.logdna.com/logs/ingest"

func init() {
	router.AdapterFactories.Register(NewLogdnaAdapter, "logdna")
}

func NewLogdnaAdapter(route *router.Route) (router.LogAdapter, error) {
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		return nil, errors.New("API_KEY not specified")
	}

	batchSize := 10
	if os.Getenv("BATCH_SIZE") != "" {
	}

	tmplStr := "{{.Data}}"
	if os.Getenv("RAW_FORMAT") != "" {
		tmplStr = os.Getenv("RAW_FORMAT")
	}
	tmpl, err := template.New("raw").Parse(tmplStr)
	if err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return &LogdnaAdapter{
		apiKey:    apiKey,
		hostname:  hostname,
		batchSize: batchSize,
		tmpl:      tmpl,
	}, nil
}

type LogdnaAdapter struct {
	apiKey    string
	hostname  string
	batchSize int
	tmpl      *template.Template
}

type Line struct {
	Timestamp int64  `json:"timestamp"`
	Line      string `json:"line"`
	File      string `json:"file"`
}

func (a *LogdnaAdapter) Stream(logstream chan *router.Message) {
	var lines []Line
	for message := range logstream {
		var buf bytes.Buffer
		err := a.tmpl.Execute(&buf, message)
		if err != nil {
			log.Fatal(err)
		}

		line := Line{time.Now().Unix(), buf.String(), ""}
		lines = append(lines, line)

		if len(lines) >= a.batchSize {
			body := struct {
				Lines []Line `json:"lines"`
			}{
				Lines: lines,
			}

			buf.Reset()
			err = json.NewEncoder(&buf).Encode(body)
			if err != nil {
				log.Fatal(err)
			}

			v := url.Values{}
			v.Add("hostname", a.hostname)
			v.Add("now", strconv.FormatInt(time.Now().Unix(), 10))

			resp, err := http.Post(API_URL+"?"+v.Encode(), "application/json; charset=UTF-8", &buf)
			if err == nil {
				defer resp.Body.Close()
			}

			lines = lines[:0]
		}
	}
}
