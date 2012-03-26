package pq

import (
	"fmt"
	"net/url"
	"strings"
)

func ParseURL(us string) (string, error) {
	u, err := url.Parse(us)
	if err != nil {
		return "", err
	}
	if u.Scheme != "postgres" {
		return "", fmt.Errorf("invalid connection protocol: %s", u.Scheme)
	}

	result := make([]string, 0, 5)
	host := ""
	switch i := strings.Index(u.Host, ":"); i {
	case -1:
		host = u.Host
	case 0:
		return "", fmt.Errorf("missing host")
	default:
		host = u.Host[:i]
		result = append(result, fmt.Sprintf("port=%s", u.Host[i+1:]))
	}
	result = append(result, fmt.Sprintf("host=%s", host))

	if u.User != nil {
		if un := u.User.Username(); un != "" {
			result = append(result, fmt.Sprintf("user=%s", un))
		}
		if p, set := u.User.Password(); set && p != "" {
			result = append(result, fmt.Sprintf("password=%s", p))
		}
	}

	if u.Path != "" && u.Path != "/" {
		result = append(result, fmt.Sprintf("dbname=%s", u.Path[1:]))
	}

	return strings.Join(result, " "), nil
}
