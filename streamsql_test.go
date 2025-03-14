package streamsql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStreamsql(t *testing.T) {
	streamsql := New()
	var rsql = ""
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
}
