package main

import (
	"math"
	"testing"
)

type testinput struct {
	input string
	lat   float64
	lon   float64
	err   string
}

var testdata = []testinput{
	testinput{
		"{{coord|61.1631|-149.9721|type:landmark_globe:earth_region:US-AK_scale:150000_source:gnis|name=Kulis Air National Guard Base}}",
		61.1631,
		-149.9721,
		"",
	},
	testinput{
		"{{coord|29.5734571|N|2.3730469|E|scale:10000000|format=dms|display=title}}",
		29.5734571,
		2.3730469,
		"",
	},
	testinput{
		"{{coord|27|59|16|N|86|56|40|E}}",
		27.98777777,
		86.94444444,
		"",
	},
	testinput{
		"{{coord|27|59|16|S|86|56|40|E}}",
		-27.98777777,
		86.94444444,
		"",
	},
	testinput{
		"{{coord|27|59|16|N|86|56|40|W}}",
		27.98777777,
		-86.94444444,
		"",
	},
	testinput{
		"{{coord|27|59|16|S|86|56|40|W}}",
		-27.98777777,
		-86.94444444,
		"",
	},
	testinput{
		"<nowiki>{{coord|27|59|16|N|86|56|40|E}}</nowiki>",
		0,
		0,
		"No coord data found.",
	},
	testinput{
		`<nowiki>
{{coord|27|59|16|N|86|56|40|E}}
</nowiki>`,
		0,
		0,
		"No coord data found.",
	},
	testinput{
		"<!-- {{coord|27|59|16|N|86|56|40|E}} -->",
		0,
		0,
		"No coord data found.",
	},
	testinput{
		`<!--
{{coord|27|59|16|N|86|56|40|E}}
-->`,
		0,
		0,
		"No coord data found.",
	},
}

func assertEpsilon(t *testing.T, input, field string, expected, got float64) {
	if math.Abs(got-expected) > 0.00001 {
		t.Fatalf("Expected %v for %v of %v, got %v",
			expected, field, input, got)
	}
}

func testOne(t *testing.T, ti testinput, input string) {
	coord, err := ParseCoords(input)
	if err != nil && err.Error() != ti.err {
		t.Fatalf("Unexpected error %q on %v, got %v", ti.err, input, err)
	}
	if err == nil && ti.err != "" {
		t.Fatalf("Expected error on %v", input)
	}
	t.Logf("Parsed %#v", coord)
	assertEpsilon(t, input, "lon", ti.lon, coord.Lon)
	assertEpsilon(t, input, "lat", ti.lat, coord.Lat)
	t.Logf("Results for %s:  %#v", input, coord)
}

func TestCoordSimple(t *testing.T) {
	for _, ti := range testdata {
		testOne(t, ti, ti.input)
	}
}

func TestCoordWithGarbage(t *testing.T) {
	for _, ti := range testdata {
		input := " some random garbage " + ti.input + " and stuff"
		testOne(t, ti, input)
	}
}

func TestCoordMultiline(t *testing.T) {
	for _, ti := range testdata {
		input := " some random garbage\n\nnewlines\n" + ti.input + " and stuff"
		testOne(t, ti, input)
	}
}
