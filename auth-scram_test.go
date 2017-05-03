package pq

import (
	"crypto/hmac"
	"encoding/hex"
	"testing"
)

func TestComputeSaltedPassword(t *testing.T) {

	salt, _ := hex.DecodeString("74172b96cd9d296b497b")
	expected_password, _ := hex.DecodeString("b58fb579cae2a50591a06a807bc0535106f8e1c725ea5ce3b6eb70ca4e2aeb99")

	salted_password := ComputeSaltedPassword("pencil", salt, 4096)

	if !hmac.Equal(salted_password, expected_password) {
		t.Error("SaltedPassword was wrong")
	}
}

func TestComputeClientProof(t *testing.T) {
	salt, _ := hex.DecodeString("31f2b148ca94a7e64554")

	salted_password := ComputeSaltedPassword("pencil", salt, 4096)
	auth_message := ComputeAuthMessage(
		[]byte("n=,r=MQiVmMEKTBZgNA=="),
		[]byte("r=MQiVmMEKTBZgNA==8zeUHmzdT2SBnQ==,s=MfKxSMqUp+ZFVA==,i=4096"),
		[]byte("c=biws,r=MQiVmMEKTBZgNA==8zeUHmzdT2SBnQ=="))

	result_ClientProof := ComputeClientProof(salted_password, auth_message)

	if result_ClientProof != "3xQR96noltaeyOY5XSNcMtogCRRZ/qJvT8ry7i9FsGs=" {
		t.Errorf("ClientProof was wrong: %s", result_ClientProof)
	}
}

func TestComputeServerSignature(t *testing.T) {
	salt, _ := hex.DecodeString("080f7c0a737897be9f0f")

	salted_password := ComputeSaltedPassword("pencil", salt, 4096)
	auth_message := ComputeAuthMessage(
		[]byte("n=,r=wDIyqexkMXIY7A=="),
		[]byte("r=wDIyqexkMXIY7A==93UKLA23FxSN9Q==,s=CA98CnN4l76fDw==,i=4096"),
		[]byte("c=biws,r=wDIyqexkMXIY7A==93UKLA23FxSN9Q=="))

	result_ServerSignature := ComputeServerSignature(salted_password, auth_message)

	if result_ServerSignature != "IeQ9HCOw5KcB8G3NunvoV9SHHUdNT8YkP/d4FAwd73g=" {
		t.Errorf("ServerSignature was wrong: %s", result_ServerSignature)
	}
}

func TestSaslPrep(t *testing.T) {
	// These tests are based on the example strings from RFC4013.txt,
	// Section "3. Examples":
	//
	// #  Input            Output     Comments
	// -  -----            ------     --------
	// 1  I<U+00AD>X       IX         SOFT HYPHEN mapped to nothing
	// 2  user             user       no transformation
	// 3  USER             USER       case preserved, will not match #2
	// 4  <U+00AA>         a          output is NFKC, input in ISO 8859-1
	// 5  <U+2168>         IX         output is NFKC, will match #1
	// 6  <U+0007>                    Error - prohibited character
	// 7  <U+0627><U+0031>            Error - bidirectional check
	test_strings := []struct {
		input    string
		expected string
		valid    bool
	}{
		{"I\u00adX", "IX", true},
		{"user", "user", true},
		{"USER", "USER", true},
		{"\u00aa", "a", true},
		{"\u2168", "IX", true},
		{"\u0007", "", false},
		{"\u0627\u0031", "", false},
	}

	for i, elem := range test_strings {
		result, err := SaslPrep(elem.input)
		if err != nil {
			if elem.valid {
				t.Errorf("input %d: SaslPrep for \"%s\": expected \"%s\", got error: %s",
					i+1, elem.input, elem.expected, err)
			}
		} else {
			if !elem.valid {
				t.Errorf("input %d: SaslPrep for \"%s\": expected error, got \"%s\"",
					i+1, elem.input, result)
			} else if result != elem.expected {
				t.Errorf("input %d: SaslPrep for \"%s\": expected \"%s\", got \"%s\"",
					i+1, elem.input, elem.expected, result)
			}
		}
	}
}
