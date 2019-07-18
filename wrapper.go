package pq

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var namedParamRegex = regexp.MustCompile(`[^:]{1}[:]{1}[A-Za-z]+`)

// requestWrapper is used to handle, if necessary, named parameters in request.
type requestWrapper struct {
	request           string
	namedParamUsed    bool
	namedParamMapping map[string]int
}

// newRequestWrapper setup a queryWrapper. During the process, the query is analyzed
// to determine if named parameters (alphabetical labels prefixed by ':') are used.
// If so the request is updated to replace named parameters by ordinal parameters and the mapping is stored.
//
// NOTE: mixing ordinal or '?' parameters with named parameter is not supported and will result in a sql error
// or in a inconsistent behavior.
func newRequestWrapper(request string) (*requestWrapper, error) {

	//var hasNamedParam bool
	res := &requestWrapper{request: request, namedParamMapping: make(map[string]int, 0)}
	matchIndexes := namedParamRegex.FindAllIndex([]byte(request), -1)

	if len(matchIndexes) == 0 {
		return res, nil
	} else {
		res.namedParamUsed = true
	}

	var currentIndex int
	parameterMapping := make(map[string]int, 1)
	matchMapping := make(map[string][]string, 1)

	for _, matchIndex := range matchIndexes {
		match := request[matchIndex[0]:matchIndex[1]]
		parameter := match[2:]

		_, present := parameterMapping[parameter]
		if !present {
			parameterMapping[parameter] = currentIndex
			matchMapping[parameter] = []string{match}
			currentIndex++
		} else {
			matchMapping[parameter] = append(matchMapping[parameter], match)
		}
	}

	for parameter, index := range parameterMapping {
		// for the replacement, we take the first character of the match (a non ':') and append it to the
		// beginning of the ordinal parameter
		ordinal := index + 1 // ordinal parameters start at 1
		for _, match := range matchMapping[parameter] {
			res.request = strings.Replace(res.request, match, match[:1]+"$"+strconv.Itoa(ordinal), -1)
		}
		res.namedParamMapping[parameter] = index
	}

	return res, nil
}

// buildParamsList translate the given driver.NamedValue slice into a driver.Value slice.
// When the underling query does not use named parameters, nothing special is done.
// when named parameters are used the returned driver.Value slice is sorted throught the
// named parameters mapping registered into that instance of queryWrapper. If a mapping error is
// detected, a non nil error is returned.
func (w *requestWrapper) buildParamsList(args []driver.NamedValue) ([]driver.Value, error) {

	list := make([]driver.Value, len(args), len(args))
	if w.namedParamUsed {

		// We try to detect many cases of binding error with named param, by comparing args length
		// with length of registered param mapping, parameter duplication or unknown parameter.
		// In all this cases a detailed (and we hope, readable) error is build and returned.
		// This effort is done to help users to debug sql code, which may be, in certain circumstances, painful.
		if len(args) != len(w.namedParamMapping) {
			count := 0
			errMsg := fmt.Sprintf("Expect %d named parameter(s) :", len(w.namedParamMapping))
			for key, _ := range w.namedParamMapping {
				if count == 0 {
					errMsg = fmt.Sprintf("%s %s", errMsg, key)
				} else {
					errMsg = fmt.Sprintf("%s, %s", errMsg, key)
				}
				count++
			}
			errMsg = fmt.Sprintf("%s. Got only", errMsg)

			for i, arg := range args {
				if i == 0 {
					errMsg = fmt.Sprintf("%s %s", errMsg, arg.Name)
				} else {
					errMsg = fmt.Sprintf("%s, %s", errMsg, arg.Name)
				}
			}
			return nil, errors.New(errMsg)
		}

		for _, nv := range args {
			pos, ok := w.namedParamMapping[nv.Name]

			if !ok {
				return nil, errors.New(fmt.Sprintf("%s param is unknown", nv.Name))
			}

			if list[pos] != nil {
				return nil, errors.New(fmt.Sprintf("Expect %s param to be only present one time", nv.Name))
			}
			list[pos] = nv.Value
		}
	} else {
		for i, nv := range args {
			list[i] = nv.Value
		}
	}
	return list, nil
}
