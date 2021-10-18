package wikiparse

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var coordRE, nowikiRE, commentRE *regexp.Regexp

// ErrNoCoordFound is returned from ParseCoords when there's no
// coordinate date found.
var ErrNoCoordFound = errors.New("no coord data found")

var errNotSexagesimal = errors.New("not a sexagesimal value")

func init() {
	coordRE = regexp.MustCompile(`(?mi){{coord\|(.[^}]*)}}`)
	nowikiRE = regexp.MustCompile(`(?ms)<nowiki>.*</nowiki>`)
	commentRE = regexp.MustCompile(`(?ms)<!--.*-->`)
}

// Coord is Longitude/latitude pair from a coordinate match.
type Coord struct {
	Lon, Lat float64
}

func dms(parts []string) (float64, error) {
	if len(parts) != 4 {
		return 0, fmt.Errorf("Wrong number of elements: %#v", parts)
	}
	rv, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return rv, err
	}
	f, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return rv, err
	}
	rv += f / 60.0
	f, err = strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return rv, err
	}
	rv += f / 3600.0

	if parts[3] == "S" || parts[3] == "W" {
		rv = -rv
	}
	return rv, err
}

func dm(parts []string) (float64, error) {
	if len(parts) != 3 {
		return 0, fmt.Errorf("Wrong number of elements: %#v", parts)
	}
	rv, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return rv, err
	}
	f, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return rv, err
	}
	rv += f / 60.0
	if parts[2] == "S" || parts[2] == "W" {
		rv = -rv
	}
	return rv, err
}

func parseSexagesimal(parts []string) (Coord, error) {
	if len(parts) < 8 {
		return Coord{}, errNotSexagesimal
	}
	if parts[3] != "N" && parts[3] != "S" {
		return Coord{}, errNotSexagesimal
	}
	if parts[7] != "E" && parts[7] != "W" {
		return Coord{}, errNotSexagesimal
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


func parseSexagesimal2(parts []string) (Coord, error) {
	if len(parts) < 6 {
		return Coord{}, errNotSexagesimal
	}
	if parts[2] != "N" && parts[2] != "S" {
		return Coord{}, errNotSexagesimal
	}
	if parts[5] != "E" && parts[5] != "W" {
		return Coord{}, errNotSexagesimal
	}

	lat, err := dm(parts[0:3])
	if err != nil {
		return Coord{}, err
	}

	lon, err := dm(parts[3:6])

	rv := Coord{
		Lat: lat,
		Lon: lon,
	}

	return rv, err
}

func parseFloat(parts []string) (Coord, error) {
	if len(parts) < 2 {
		return Coord{}, ErrNoCoordFound
	}

	offset := 0

	lat, err := strconv.ParseFloat(parts[offset], 64)
	if err != nil {
		return Coord{}, err
	}
	offset++

	switch parts[offset] {
	case "S":
		lat = -lat
		fallthrough
	case "N":
		offset++
	}

	lon, err := strconv.ParseFloat(parts[offset], 64)
	offset++
	if len(parts) > offset && parts[offset] == "W" {
		lon = -lon
	}
	return Coord{Lat: lat, Lon: lon}, err
}

func cleanCoordParts(in []string) []string {
	out := make([]string, 0, len(in))

	firstnumber := 0
	var part string
	for firstnumber, part = range in {
		_, e := strconv.ParseFloat(part, 64)
		if e == nil {
			break
		}
	}

	for _, p := range in[firstnumber:] {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}

	return out
}

// ParseCoords parses geographical coordinates as specified in
// http://en.wikipedia.org/wiki/Wikipedia:WikiProject_Geographical_coordinates
func ParseCoords(text string) (Coord, error) {
	cleaned := nowikiRE.ReplaceAllString(commentRE.ReplaceAllString(text, ""), "")
	matches := coordRE.FindAllStringSubmatch(cleaned, 1)

	if len(matches) == 0 || len(matches[0]) < 2 {
		return Coord{}, ErrNoCoordFound
	}

	parts := cleanCoordParts(strings.Split(matches[0][1], "|"))

	rv, err := parseSexagesimal(parts)
	if err != nil {
		rv, err = parseSexagesimal2(parts)
		if err != nil {
			rv, err = parseFloat(parts)
		}
	}

	if math.Abs(rv.Lat) > 90 {
		return rv, fmt.Errorf("invalid latitude: %v", rv.Lat)
	}
	if math.Abs(rv.Lon) > 180 {
		return rv, fmt.Errorf("invalid longitude: %v", rv.Lon)
	}

	return rv, err
}
