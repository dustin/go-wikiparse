package wikiparse

import (
	"regexp"
)

var linkRE *regexp.Regexp

func init() {
	linkRE = regexp.MustCompile(`\[\[([^\|\]]+)`)
}

// FindLinks finds all the links from within an article body.
func FindLinks(text string) []string {
	cleaned := nowikiRE.ReplaceAllString(commentRE.ReplaceAllString(text, ""), "")
	matches := linkRE.FindAllStringSubmatch(cleaned, -1)

	rv := make([]string, 0, len(matches))
	for _, x := range matches {
		rv = append(rv, x[1])
	}

	return rv
}
