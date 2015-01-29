package json

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

type Fatalistic interface {
	Fatal(args ...interface{})
}

func openTestConn(t Fatalistic) *sql.DB {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	conn, err := sql.Open("postgres", "")
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func TestScan(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec("SELECT null::json")
	if err != nil {
		t.Skipf("Skipping JSON tests: %s", err.Error())
	}

	jsonValue := JSON{}

	// Test null values
	err = db.QueryRow("SELECT null::json").Scan(&jsonValue)
	if err != nil {
		t.Fatal(err)
	}

	if jsonValue.Raw != nil {
		t.Fatalf("expected empty value")
	}

	// Test integer values
	err = db.QueryRow(`SELECT '1'::json`).Scan(&jsonValue)
	if err != nil {
		t.Fatal(err)
	}

	expectedIntValue := 0

	err = json.Unmarshal(jsonValue.Raw, &expectedIntValue)
	if err != nil {
		t.Fatal(err)
	}

	if expectedIntValue != 1 {
		t.Fatal("Expected integer value to be correct")
	}

	// Test string values
	err = db.QueryRow(`SELECT '"hello"'::json`).Scan(&jsonValue)
	if err != nil {
		t.Fatal(err)
	}

	expectedStringValue := ""

	err = json.Unmarshal(jsonValue.Raw, &expectedStringValue)
	if err != nil {
		t.Fatal(err)
	}

	if expectedStringValue != "hello" {
		t.Fatal("Expected string value to be correct")
	}

	// Test float values
	err = db.QueryRow(`SELECT '5.5'::json`).Scan(&jsonValue)
	if err != nil {
		t.Fatal(err)
	}

	expectedFloatValue := 0.0

	err = json.Unmarshal(jsonValue.Raw, &expectedFloatValue)
	if err != nil {
		t.Fatal(err)
	}

	if expectedFloatValue != 5.5 {
		t.Fatal("Expected float value to be correct")
	}

	// Test map values
	err = db.QueryRow(`SELECT '{"foo": "bar"}'::json`).Scan(&jsonValue)
	if err != nil {
		t.Fatal(err)
	}

	expectedMapValue := map[string]string{}

	err = json.Unmarshal(jsonValue.Raw, &expectedMapValue)
	if err != nil {
		t.Fatal(err)
	}

	if expectedMapValue["foo"] != "bar" {
		t.Fatal("Expected map value to be correct")
	}

	// Test struct values
	err = db.QueryRow(`SELECT '{"bar": "baz", "balance": 7.77, "active": true}'::json`).Scan(&jsonValue)
	if err != nil {
		t.Fatal(err)
	}

	var expectedStructValue struct {
		Bar     string
		Balance float64
		Active  bool
	}

	err = json.Unmarshal(jsonValue.Raw, &expectedStructValue)
	if err != nil {
		t.Fatal(err)
	}

	if expectedStructValue.Bar != "baz" {
		t.Fatal("Expected struct value to be correct")
	}

	if expectedStructValue.Balance != 7.77 {
		t.Fatal("Expected struct value to be correct")
	}

	if expectedStructValue.Active != true {
		t.Fatal("Expected struct value to be correct")
	}
}

func TestValue(t *testing.T) {
	// Nil value
	jsonValue := JSON{}

	v, err := jsonValue.Value()
	if err != nil {
		t.Fatal(err)
	}

	if v != nil {
		t.Fatal("Expected nil value")
	}

	// Invalid JSON format
	jsonValue.Raw = []byte(`{"bar": "baz", "balance": 7.77, "active": false`)
	_, err = jsonValue.Value()
	if err == nil {
		t.Fatal("An error was expected")
	}

	// Valid JSON format
	jsonValue.Raw = []byte(`{"bar": "baz", "balance": 7.77, "active": false}`)
	_, err = jsonValue.Value()
	if err != nil {
		t.Fatal(err)
	}

}
