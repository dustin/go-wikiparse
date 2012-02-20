package main

import (
	"math"
	"testing"
)

type testinput struct {
	input string
	gtype string
	scale string
	lon   float64
	lat   float64
}

var testdata = []testinput{
	testinput{
		"{{Geolinks-US-streetscale|34.1996350|-118.1746540}}",
		"US", "streetscale",
		-118.1746540,
		34.1996350,
	},
	testinput{
		"{{Geolinks-AUS-suburbscale|long=146.5333|lat=-38.1833}}",
		"AUS", "suburbscale",
		146.5333,
		-38.1833,
	},
	testinput{
		"{{geolinks-US-streetscale|37.2750|-81.1240|region:US_type:_scale:300000}}",
		"US", "streetscale",
		-81.1240,
		37.2750,
	},
	testinput{
		"{{Geolinks-US-buildingscale|30.325939|-87.316879}}",
		"US", "buildingscale",
		-87.316879,
		30.325939,
	},
	testinput{
		"{{geolinks-Canada-streetscale|45.375121|-75.897846}}",
		"Canada", "streetscale",
		-75.897846,
		45.375121,
	},
	testinput{
		"{{Geolinks-US-streetscale|39.118474|-77.235947|Kentlands (Gaithersburg, MD)}}",
		"US", "streetscale",
		-77.235947,
		39.118474,
	},
	testinput{
		"{{Geolinks-US-streetscale|40.94759700 |-72.89820700}}",
		"US", "streetscale",
		-72.89820700,
		40.94759700,
	},
	testinput{
		"{{Geolinks-AUS-suburbscale|lat=-25.898938|long=139.351694}}",
		"AUS", "suburbscale",
		139.351694,
		-25.898938,
	},
}

func assertEpsilon(t *testing.T, input, field string, expected, got float64) {
	if math.Abs(got-expected) > 0.00001 {
		t.Fatalf("Expected %v for %v of %v, got %v",
			expected, field, input, got)
	}
}

func testOne(t *testing.T, ti testinput, input string) {
	geo, err := ParseGeolinks(input)
	if err != nil {
		t.Fatalf("Error on %v: %v", input, err)
	}
	t.Logf("Parsed %#v", geo)
	if geo.Type != ti.gtype {
		t.Fatalf("Expected type %v for %v, got %v",
			ti.gtype, input, geo.Type)
	}
	if geo.Scale != ti.scale {
		t.Fatalf("Expected scale %v for %v, got %v",
			ti.scale, input, geo.Scale)
	}
	assertEpsilon(t, input, "lon", ti.lon, geo.Lon)
	assertEpsilon(t, input, "lat", ti.lat, geo.Lat)
	t.Logf("Results for %s:  %#v", input, geo)
}

func TestGeoSimple(t *testing.T) {
	for _, ti := range testdata {
		testOne(t, ti, ti.input)
	}
}

func TestGeoWithGarbage(t *testing.T) {
	for _, ti := range testdata {
		input := " some random garbage " + ti.input + " and stuff"
		testOne(t, ti, input)
	}
}

func TestGeoMultiline(t *testing.T) {
	for _, ti := range testdata {
		input := " some random garbage\n\nnewlines\n" + ti.input + " and stuff"
		testOne(t, ti, input)
	}
}
