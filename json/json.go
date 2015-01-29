package json

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type JSON struct {
	Raw json.RawMessage
}

func (jv *JSON) Scan(value interface{}) error {
	var source []byte
	switch value.(type) {
	case string:
		source = []byte(value.(string))
	case []byte:
		source = value.([]byte)
	case nil:
		source = nil
	default:
		return errors.New("Incompatible value for JSON type")
	}

	jv.Raw = append(jv.Raw[0:0], source...)
	return nil
}

func (jv JSON) Value() (driver.Value, error) {
	if jv.Raw == nil {
		return nil, nil
	}

	var validator json.RawMessage

	err := json.Unmarshal(jv.Raw, &validator)

	if err != nil {
		return jv.Raw, err
	}

	return jv.Raw, nil
}
