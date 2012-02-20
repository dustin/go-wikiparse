package main

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var coordRE, nowikiRE, commentRE *regexp.Regexp

var NoCoordFound = errors.New("No coord data found.")

var notSexagesimal = errors.New("Not a sexagesimal value")

func init() {
	coordRE = regexp.MustCompile(`(?mi){{coord\|(.[^}]*)}}`)
	nowikiRE = regexp.MustCompile(`(?ms)<nowiki>.*</nowiki>`)
	commentRE = regexp.MustCompile(`(?ms)<!--.*-->`)
}

type Coord struct {
	Lon float64
	Lat float64
}

func dms(parts []string) (rv float64, err error) {
	if len(parts) != 4 {
		panic(fmt.Sprintf("Wrong number of elements: %#v", parts))
	}
	var f float64
	f, err = strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return
	}
	rv = f
	f, err = strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return
	}
	rv += f / 60.0
	f, err = strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return
	}
	rv += f / 3600.0

	if parts[3] == "S" || parts[3] == "W" {
		rv = -rv
	}
	return
}

func parseSexagesimal(parts []string) (Coord, error) {
	if len(parts) < 8 {
		return Coord{}, notSexagesimal
	}
	if parts[3] != "N" && parts[3] != "S" {
		return Coord{}, notSexagesimal
	}
	if parts[7] != "E" && parts[7] != "W" {
		return Coord{}, notSexagesimal
	}

	lat, err := dms(parts[0:4])
	if err != nil {
		return Coord{}, err
	}

	lon, err := dms(parts[4:8])

	rv := Coord{
		Lat: lat,
		Lon: lon,
	}

	return rv, err
}

func parseFloat(parts []string) (rv Coord, err error) {
	if len(parts) < 2 {
		return Coord{}, NoCoordFound
	}

	offset := 0

	rv.Lat, err = strconv.ParseFloat(parts[offset], 64)
	if err != nil {
		return
	}
	offset++

	if parts[offset] == "S" {
		rv.Lat = -rv.Lat
		offset++
	} else if parts[offset] == "N" {
		offset++
	}

	rv.Lon, err = strconv.ParseFloat(parts[offset], 64)
	offset++
	if len(parts) > offset && parts[offset] == "W" {
		rv.Lon = -rv.Lon
	}
	return
}

/*
Parses geographical coordinates as specified in
http://en.wikipedia.org/wiki/Wikipedia:WikiProject_Geographical_coordinates
*/
func ParseCoords(text string) (rv Coord, err error) {
	cleaned := nowikiRE.ReplaceAllString(commentRE.ReplaceAllString(text, ""), "")
	matches := coordRE.FindAllStringSubmatch(cleaned, 1)

	if len(matches) == 0 || len(matches[0]) < 2 {
		return Coord{}, NoCoordFound
	}

	parts := strings.Split(matches[0][1], "|")

	for i, _ := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	firstnumber := 0
	var part string
	for firstnumber, part = range parts {
		_, e := strconv.ParseFloat(part, 64)
		if e == nil {
			break
		}
	}

	rv, err = parseSexagesimal(parts[firstnumber:])
	if err != nil {
		rv, err = parseFloat(parts[firstnumber:])
	}

	if err == nil {
		if math.Abs(rv.Lat) > 90 {
			return rv, errors.New(fmt.Sprintf("Invalid latitude: %v", rv.Lat))
		}
		if math.Abs(rv.Lon) > 180 {
			return rv, errors.New(fmt.Sprintf("Invalid longitude: %v", rv.Lon))
		}
	} else {
		rv = Coord{}
	}

	return rv, err
}
