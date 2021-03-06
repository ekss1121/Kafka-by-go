package client

import (
	"bytes"
	"testing"
)

func TestCutTolastMessage(t *testing.T) {
	res := []byte("100\n101\n10")

	wantTruncated, wantRest := []byte("100\n101\n"), []byte("10")
	gotTruncated, gotRest, err := cutToLastMessage(res)
	if err != nil {
		 t.Errorf("Error when truncate message %q. got: %q, %q, error: %v. want: no error", string(res),
			 string(gotTruncated), string(gotRest), err)
	}

	if !bytes.Equal(wantTruncated, gotTruncated) || !bytes.Equal(wantRest, gotRest) {
		t.Errorf("Error when truncate message %q. got: %q, %q. want: %q, %q", string(res),
			string(gotTruncated), string(gotRest), string(wantTruncated), string(wantRest))
	}
}

func TestCutTolastMessageErrors(t *testing.T) {
	res := []byte("10010110")

	_, _, err:= cutToLastMessage(res)
	if err == nil {
		t.Errorf("Should throw errors but no error got")
	}
}
