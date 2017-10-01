package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"regexp"
)

var config struct {
	impalaURL string
}

func trackHandler(w http.ResponseWriter, r *http.Request) {
	trackRe := regexp.MustCompile("^/track/(?P<uuid>[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}).flac$")
	match := trackRe.FindStringSubmatch(r.URL.Path)
	if len(match) < 2 {
		http.NotFound(w, r)
		return
	}

	// make the request to impala
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/tracks/%s", config.impalaURL, match[1]))
	if err != nil {
		http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)

	// TODO: need a value for this
	err = decoder.Decode(nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
		return
	}

	// FIXME
	http.Error(w, match[1], http.StatusInternalServerError)
}

func main() {
	flag.StringVar(&config.impalaURL, "impalaurl", "",
		"URL to IMPALA instance")
	flag.Parse()

	if config.impalaURL == "" {
		log.Fatal("URL to IMPALA instance must be provided.")
	}

	http.HandleFunc("/track/", trackHandler)
	http.ListenAndServe(":8080", nil)
}
