package wikiparse

import (
	"math"
	"strings"
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
		"{{Coord|display=title|45|N|114|W|region:US-ID_type:adm1st_scale:3000000}}",
		45,
		-114,
		"",
	},
	testinput{
		"{{Coord|42||N|82||W|}}",
		42,
		-82,
		"",
	},
	testinput{
		"{{Coord|42||S|82||W|}}",
		-42,
		-82,
		"",
	},
	testinput{
		"{{Coord|display=title|41.762736| -72.674286}}",
		41.762736,
		-72.674286,
		"",
	},
	testinput{
		"North Maple in Russell ({{coord|38.895352|-98.861034}}) and it remained his " +
			"official residence throughout his political career." +
			"<ref>{{cite news| url=http://www.time.com/}}",
		38.895352,
		-98.861034,
		"",
	},
	testinput{
		"{{coord|97|59|16|S|86|56|40|W|invalid lat}}",
		-97.98777777,
		-86.94444444,
		"invalid latitude: -97.98777",
	},
	testinput{
		"{{coord|27|59|16|S|186|56|40|W|invalid long}}",
		-27.98777777,
		-186.94444444,
		"invalid longitude: -186.9444",
	},
	testinput{
		"<nowiki>{{coord|27|59|16|N|86|56|40|E}}</nowiki>",
		0,
		0,
		"no coord data found",
	},
	testinput{
		`<nowiki>
{{coord|27|59|16|N|86|56|40|E}}
</nowiki>`,
		0,
		0,
		"no coord data found",
	},
	testinput{
		"<!-- {{coord|27|59|16|N|86|56|40|E}} -->",
		0,
		0,
		"no coord data found",
	},
	testinput{
		`<!--
{{coord|27|59|16|N|86|56|40|E}}
-->`,
		0,
		0,
		"no coord data found",
	},
	// The following two fall back onto float parsing
	testinput{
		"{{coord|27|59|16|J|86|56|40|E}}",
		27,
		59,
		"",
	},
	testinput{
		"{{coord|27|59|16|N|86|56|40|J}}",
		27,
		59,
		"",
	},
	// And this should fail dms, but coverage suggests it doesn't
	/*
		testinput{
			"{{coord|foo|59|foo|N|86|56|40|S}}",
			59,
			0,
			`strconv.ParseFloat: parsing "foo": invalid syntax`,
		},
	*/
}

func assertEpsilon(t *testing.T, input, field string, expected, got float64) {
	if math.Abs(got-expected) > 0.00001 {
		t.Fatalf("Expected %v for %v of %v, got %v",
			expected, field, input, got)
	}
}

func testOne(t *testing.T, ti testinput, input string) {
	coord, err := ParseCoords(input)
	switch {
	case err != nil && ti.err == "":
		t.Fatalf("Unexpected error on %v, got %v, wanted %q", input, err, ti.err)
	case err != nil && strings.HasPrefix(err.Error(), ti.err):
		// ok
	case err == nil && ti.err == "":
		// ok
	case err == nil && ti.err != "":
		t.Fatalf("Expected error %q on %v, got %v", ti.err, input, coord)
	default:
		t.Fatalf("Wanted %v,%v with error %v, got %#v with error %v",
			ti.lat, ti.lon, ti.err, coord, err)
	}
	assertEpsilon(t, input, "lon", ti.lon, coord.Lon)
	assertEpsilon(t, input, "lat", ti.lat, coord.Lat)
}

func TestCoordSimple(t *testing.T) {
	t.Parallel()
	for _, ti := range testdata {
		testOne(t, ti, ti.input)
	}
}

func TestCoordWithGarbage(t *testing.T) {
	t.Parallel()
	for _, ti := range testdata {
		input := " some random garbage " + ti.input + " and stuff"
		testOne(t, ti, input)
	}
}

func TestCoordMultiline(t *testing.T) {
	t.Parallel()
	for _, ti := range testdata {
		input := " some random garbage\n\nnewlines\n" + ti.input + " and stuff"
		testOne(t, ti, input)
	}
}

func TestDMS(t *testing.T) {
	tests := []struct {
		input   []string
		exp     float64
		success bool
	}{
		{nil, 0, false},
		{[]string{"x", "0", "0", "0"}, 0, false},
		{[]string{"0", "x", "0", "0"}, 0, false},
		{[]string{"0", "0", "x", "0"}, 0, false},
		{[]string{"0", "0", "0", "0"}, 0, true},
	}

	for _, test := range tests {
		f, err := dms(test.input)
		if test.success && err != nil {
			t.Errorf("Unexpected failure on %v: %v", test.input, err)
		} else if test.success && f != test.exp {
			t.Errorf("Expected %v for %v, got %v", test.exp, test.input, f)
		}
	}
}

func TestFloatErrorCases(t *testing.T) {
	tests := [][]string{
		[]string{"13"},
		[]string{"X", "N"},
	}
	for _, test := range tests {
		f, err := parseFloat(test)
		if err == nil {
			t.Errorf("expected error for %v, got %v", test, f)
		}
	}
}
