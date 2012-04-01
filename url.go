package pq

import (
	"fmt"
	nurl "net/url"
	"strings"
	"sort"
)

// ParseURL converts url to a connection string for driver.Open.
// Example:
//
//	"postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
//
// converts to:
//
//	"user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
//
// A minimal example:
//
//	"postgres://"
//
// This will be blank, causing driver.Open to use all of the defaults
func ParseURL(url string) (string, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return "", err
	}

	if u.Scheme != "postgres" {
		return "", fmt.Errorf("invalid connection protocol: %s", u.Scheme)
	}

	var kvs []string
	if u.User != nil {
		v := u.User.Username()
		kvs = appendkv(kvs, "user", v)

		v, _ =  u.User.Password()
		kvs = appendkv(kvs, "password", v)
	}

	i := strings.Index(u.Host, ":")
	if i < 0 {
		kvs = appendkv(kvs, "host", u.Host)
	} else {
		kvs = appendkv(kvs, "host", u.Host[:i])
		kvs = appendkv(kvs, "port", u.Host[i+1:])
	}

	if u.Path != "" {
		kvs = appendkv(kvs, "dbname", u.Path[1:])
	}

	q := u.Query()
	for k, _ := range q {
		kvs = appendkv(kvs, k, q.Get(k))
	}
	
	sort.Strings(kvs) // Makes testing easier (not a performance concern)
	return strings.Join(kvs, " "), nil
}

func appendkv(kvs []string, k, v string) []string {
	if v != "" {
		return append(kvs, k+"="+v)
	}
	return kvs
}
