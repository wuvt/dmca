package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"golang.org/x/net/publicsuffix"
)

var config struct {
	impalaURL      string
	impalaUsername string
	impalaPassword string
	mossURL        string
}

type TrackNotFoundError struct {
	ID string
}

func (e TrackNotFoundError) Error() string {
	return fmt.Sprintf("Track not found: %v", e.ID)
}

type TrackFetchError struct {
	ID string
}

func (e TrackFetchError) Error() string {
	return fmt.Sprintf("Error fetching track information: %v", e.ID)
}

type TrackJSON struct {
	Added_At       string
	Added_By       string
	Artist         string
	Disc_Num       uint64
	File_Path      string
	Has_FCC        string
	Holding_ID     string
	ID             string
	Recording_MBID string
	Title          string
	Track_MBID     string
	Track_Num      uint64
}

func loadTrackInfo(trackID string) (data *TrackJSON, err error) {
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return
	}

	client := &http.Client{
		Jar: jar,
	}

	loginReq, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/login", config.impalaURL), nil)
	if err != nil {
		return
	}
	loginReq.SetBasicAuth(config.impalaUsername, config.impalaPassword)
	loginResp, err := client.Do(loginReq)
	if err != nil {
		return
	}
	defer loginResp.Body.Close()

	// make the request to impala
	resp, err := client.Get(fmt.Sprintf("%s/api/v1/tracks/%s", config.impalaURL, trackID))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		err = TrackNotFoundError{trackID}
		return
	} else if resp.StatusCode != 200 {
		err = TrackFetchError{trackID}
		return
	}

	data = &TrackJSON{}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(data)
	return
}

func trackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	trackRe := regexp.MustCompile("^/track/(?P<uuid>[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}).flac$")
	match := trackRe.FindStringSubmatch(r.URL.Path)
	if len(match) < 2 {
		http.NotFound(w, r)
		return
	}

	track, err := loadTrackInfo(match[1])
	if _, ok := err.(TrackNotFoundError); ok {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Failed to load track information: %v\n", err)
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/%s/music/%s", config.mossURL, url.PathEscape(track.Holding_ID), url.PathEscape(track.File_Path)))
	if err != nil || resp.StatusCode != 200 {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Failed to access track: %v\n", err)
		return
	}
	defer resp.Body.Close()

	tmpf, err := ioutil.TempFile("", "dmca")
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Failed to create temporary file: %v\n", err)
		return
	}
	defer os.Remove(tmpf.Name())

	if _, err := io.Copy(tmpf, resp.Body); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Failed to download track: %v\n", err)
		return
	}
	if err := tmpf.Close(); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Failed to close temporary file: %v\n", err)
		return
	}

	cmd := exec.Command("metaflac", "--remove-tag=ARTIST", "--remove-tag=TITLE", "--remove-tag=ALBUM", "--remove-tag=LABEL", "--import-tags-from=-", tmpf.Name())
	// FIXME: actually use the data from impala here for the album and label
	cmd.Stdin = strings.NewReader(fmt.Sprintf("ARTIST=%s\nTITLE=%s\nALBUM=dmca test\nLABEL=dmca test\n", track.Artist, track.Title))
	if err := cmd.Run(); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Failed to modify track metadata: %v\n", err)
		return
	}

	f, err := os.Open(tmpf.Name())
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Failed to load track: %v\n", err)
		return
	}
	defer f.Close()

	http.ServeContent(w, r, fmt.Sprintf("%s.flac", track.ID), time.Now(), f)
}

func main() {
	flag.StringVar(&config.impalaURL, "impalaurl", "",
		"URL to IMPALA instance")
	flag.StringVar(&config.impalaUsername, "impalauser", "",
		"Username to use for IMPALA access")
	flag.StringVar(&config.impalaPassword, "impalapassword", "",
		"Password to use for IMPALA access")
	flag.StringVar(&config.mossURL, "mossurl", "",
		"URL to MOSS instance")
	flag.Parse()

	if config.impalaURL == "" {
		log.Fatal("URL to IMPALA instance must be provided.")
	}

	http.HandleFunc("/track/", trackHandler)
	http.ListenAndServe(":8080", nil)
}
