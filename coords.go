package main

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var GeoRE *regexp.Regexp

var NoGeoFound = errors.New("No geolinks data found.")

func init() {
	GeoRE = regexp.MustCompile(`(?mi){{geolinks-(\w+)-(\w+)\|([^}]*)}}`)
}

type Geolink struct {
	Type  string
	Scale string
	Lon   float64
	Lat   float64
}

// Fill when a simple lat/long pair
func fillLatLon(rv *Geolink, latlon []string) error {
	f, err := strconv.ParseFloat(latlon[0], 64)
	if err != nil {
		return err
	}
	rv.Lat = f

	rv.Lon, err = strconv.ParseFloat(latlon[1], 64)

	return err
}

func fillLongLat(rv *Geolink, longlat []string) error {
	if !strings.HasPrefix(longlat[0], "long=") {
		return errors.New("Unhandled case.")
	}
	if !strings.HasPrefix(longlat[1], "lat=") {
		return errors.New("Unhandled case.")
	}

	f, err := strconv.ParseFloat(strings.Split(longlat[0], "=")[1], 64)
	if err != nil {
		return err
	}
	rv.Lon = f

	rv.Lat, err = strconv.ParseFloat(strings.Split(longlat[1], "=")[1], 64)
	return err
}

func ParseGeolinks(text string) (rv Geolink, err error) {
	matches := GeoRE.FindAllStringSubmatch(text, 1)
	if len(matches) == 0 || len(matches[0]) < 4 {
		return rv, NoGeoFound
	}
	rv.Type = matches[0][1]
	rv.Scale = matches[0][2]
	latlon := strings.Split(matches[0][3], "|")
	if len(latlon) < 2 {
		return rv, NoGeoFound
	}

	if strings.HasPrefix(latlon[0], "long=") {
		err = fillLongLat(&rv, latlon)
	} else {
		err = fillLatLon(&rv, latlon)
	}

	return rv, err
}
