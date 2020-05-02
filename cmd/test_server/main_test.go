package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatusz(t *testing.T) {
	assert := assert.New(t)

	s := testServer{dispatch: func(dbName, rpc string, data []byte) (ret []byte, err error) {
		return []byte(""), nil
	}}

	req := httptest.NewRequest("GET", "http://example.com/statusz", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	assert.Equal(http.StatusOK, w.Result().StatusCode)
}

func TestMethod(t *testing.T) {
	assert := assert.New(t)

	s := testServer{dispatch: func(dbName, rpc string, data []byte) (ret []byte, err error) {
		return []byte(""), nil
	}}

	req := httptest.NewRequest("GET", "http://example.com/?", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	assert.Equal(http.StatusMethodNotAllowed, w.Result().StatusCode)
}

func TestDispatch(t *testing.T) {
	assert := assert.New(t)

	tc := []struct {
		dbName        string
		rpc           string
		data          string
		expectedError error
		expectedRet   string
	}{
		{
			"dbname",
			"rpc",
			"data",
			nil,
			"ret",
		},
		{
			"dbname2",
			"rpc2",
			"data2",
			errors.New("error 123"),
			"ret2",
		},
	}

	for _, t := range tc {
		s := testServer{dispatch: func(dbName, rpc string, data []byte) (ret []byte, err error) {
			assert.Equal(t.dbName, dbName)
			assert.Equal(t.rpc, rpc)
			assert.Equal([]byte(t.data), data)
			return []byte(t.expectedRet), t.expectedError
		}}

		req := httptest.NewRequest("POST", fmt.Sprintf("http://example.com/?dbname=%s&rpc=%s", t.dbName, t.rpc), strings.NewReader(t.data))
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)

		if t.expectedError == nil {
			assert.Equal(http.StatusOK, resp.StatusCode)
			assert.Equal([]byte(t.expectedRet), body)
		} else {
			assert.Equal(http.StatusInternalServerError, resp.StatusCode)
			assert.Equal(fmt.Sprintf("Failed to dispatch %s: %s", t.rpc, t.expectedError.Error()), string(body))
		}

	}
}
