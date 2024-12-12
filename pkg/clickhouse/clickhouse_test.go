package clickhouse

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/nameindexer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalIndexSlice_Success(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	input := []any{
		"did:dimo:vehicle123",
		testTime,
		"status",
		"0x123",
		"1.0",
		"secondary",
		"did:dimo:producer456",
		"optional data",
		"index_key_789",
	}

	jsonData, err := json.Marshal(input)
	require.NoError(t, err)

	got, err := UnmarshalIndexSlice(jsonData)
	require.NoError(t, err)

	assert.Equal(t, "did:dimo:vehicle123", got[0])
	assert.Equal(t, testTime, got[1])
	assert.Equal(t, "status", got[2])
	assert.Equal(t, "0x123", got[3])
	assert.Equal(t, "1.0", got[4])
	assert.Equal(t, "secondary", got[5])
	assert.Equal(t, "did:dimo:producer456", got[6])
	assert.Equal(t, "optional data", got[7])
	assert.Equal(t, "index_key_789", got[8])
}

func TestUnmarshalIndexSlice_InvalidJSON(t *testing.T) {
	invalidInputs := [][]byte{
		[]byte(`invalid json`),
		[]byte(`{}`),
		[]byte(`[]`),
		[]byte(`null`),
	}

	for _, input := range invalidInputs {
		got, err := UnmarshalIndexSlice(input)
		require.Error(t, err)
		require.Nil(t, got)
	}
}

func TestUnmarshalIndexSlice_InvalidTypes(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		input     []interface{}
		errorText string
	}{
		{
			name: "invalid subject type",
			input: []interface{}{
				123,
				testTime,
				"status",
				"0x123",
				"1.0",
				"secondary",
				"did:dimo:producer456",
				"optional data",
				"index_key_789",
			},
			errorText: "failed to unmarshal subject",
		},
		{
			name: "invalid timestamp",
			input: []interface{}{
				"did:dimo:vehicle123",
				"invalid time",
				"status",
				"0x123",
				"1.0",
				"secondary",
				"did:dimo:producer456",
				"optional data",
				"index_key_789",
			},
			errorText: "failed to unmarshal timestamp",
		},
		{
			name: "invalid primary filler",
			input: []interface{}{
				"did:dimo:vehicle123",
				testTime,
				true,
				"0x123",
				"1.0",
				"secondary",
				"did:dimo:producer456",
				"optional data",
				"index_key_789",
			},
			errorText: "failed to unmarshal primary filler",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonData, err := json.Marshal(tt.input)
			require.NoError(t, err)

			got, err := UnmarshalIndexSlice(jsonData)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorText)
			assert.Nil(t, got)
		})
	}
}

func TestIndexToSlice_RoundTrip(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	originalIndex := &nameindexer.Index{
		Subject:         "did:dimo:vehicle123",
		Timestamp:       testTime,
		PrimaryFiller:   "status",
		Source:          "0x123",
		DataType:        "1.0",
		SecondaryFiller: "secondary",
		Producer:        "did:dimo:producer456",
		Optional:        "optional data",
	}

	// Convert to slice
	slice, err := IndexToSlice(originalIndex)
	require.NoError(t, err)

	// Convert to JSON
	jsonData, err := json.Marshal(slice)
	require.NoError(t, err)

	// Convert back to slice
	recoveredSlice, err := UnmarshalIndexSlice(jsonData)
	require.NoError(t, err)

	// Verify all fields match
	require.Len(t, recoveredSlice, len(slice))
	assert.Equal(t, slice[0], recoveredSlice[0])
	assert.Equal(t, slice[1], recoveredSlice[1])
	assert.Equal(t, slice[2], recoveredSlice[2])
	assert.Equal(t, slice[3], recoveredSlice[3])
	assert.Equal(t, slice[4], recoveredSlice[4])
	assert.Equal(t, slice[5], recoveredSlice[5])
	assert.Equal(t, slice[6], recoveredSlice[6])
	assert.Equal(t, slice[7], recoveredSlice[7])
	assert.Equal(t, slice[8], recoveredSlice[8])
}

func TestIndexToSliceWithKey_RoundTrip(t *testing.T) {
	testTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	originalIndex := &nameindexer.Index{
		Subject:         "did:dimo:vehicle123",
		Timestamp:       testTime,
		PrimaryFiller:   "status",
		Source:          "0x123",
		DataType:        "1.0",
		SecondaryFiller: "secondary",
		Producer:        "did:dimo:producer456",
		Optional:        "optional data",
	}

	// Generate key first
	key, err := nameindexer.EncodeIndex(originalIndex)
	require.NoError(t, err)

	// Create slice with pre-encoded key
	slice := IndexToSliceWithKey(originalIndex, key)

	// Convert to JSON
	jsonData, err := json.Marshal(slice)
	require.NoError(t, err)

	// Convert back to slice
	recoveredSlice, err := UnmarshalIndexSlice(jsonData)
	require.NoError(t, err)

	// Verify the key matches
	assert.Equal(t, key, recoveredSlice[8])
}
