package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"

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

	data, err := loadTrackInfo(match[1])
	if _, ok := err.(TrackNotFoundError); ok {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Unable to load track information: %v\n", err)
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/%s/music/%s", config.mossURL, url.PathEscape(data.Holding_ID), url.PathEscape(data.File_Path)))
	if err != nil || resp.StatusCode != 200 {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Unable to access track: %v\n", err)
	}
	defer resp.Body.Close()

	magic := []byte("fLaC")

	// check that the file we're reading starts with fLaC
	var fileMagic [4]byte
	_, err = io.ReadFull(resp.Body, fileMagic[:])
	if err != nil || !bytes.Equal(fileMagic[:], magic) {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Unable to decode track: %v\n", err)
		return
	}

	// now that we've done the preliminary check, we can start writing a
	// response
	w.Header().Add("Content-Type", "audio/flac")
	w.Write(magic)

	// copy the STREAMINFO block; per the FLAC specification, the metadata
	// block header is 0x4 bytes long and the STREAMINFO block itself is
	// 0x22 bytes long
	if _, err := io.CopyN(w, resp.Body, 0x26); err != nil {
		log.Printf("Unable to decode track: %v\n", err)
		return
	}

	// TODO: walk through each METADATA block. we know when we're at the
	// end when the last metadata block flag is set to "1"
	// look for VORBIS_COMMENT block (type 4)
	// then look to see if a PADDING block (type 1) that follows it
	// if no VORBIS_COMMENT block is found or there is no PADDING block of
	// sufficient (how big?) size, then more extensive metadata rewriting
	// logic will be necessary

	// the metadata block header is 0x4 bytes long and the last field
	// contains the length of the rest of the block
	// make a struct for the metadata block header
	// for the vorbis comment block, there's a vendor field and then a tags
	// field that is just a list of all the vorbis comment structs

	// if we've reached the last metadata block and haven't found the
	// vorbis comments metadata block, add one now (and consider adding
	// padding too). remember to set the last block flag appropriately

	// finally, return the rest of the file, including the actual audio
	// frame data
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("Unable to decode track: %v\n", err)
		return
	}
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
