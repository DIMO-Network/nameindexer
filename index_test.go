package nameindexer

import (
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestEncodeIndex(t *testing.T) {
	tests := []struct {
		name      string
		input     *Index
		expected  string
		expectErr bool
	}{
		{
			name: "Valid index encoding with default fillers",
			input: &Index{
				Timestamp:       time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "",
				DataType:        "Stat/2.0.0",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "",
			},
			expected:  "759388MMStat/2.0.0c57d6d57fca59d0517038c968a1b831b071fa67900153000",
			expectErr: false,
		},
		{
			name: "Valid index encoding with custom fillers",
			input: &Index{
				Timestamp:       time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "XX",
				DataType:        "Stat/2.0.0",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "99",
			},
			expected:  "759388XXStat/2.0.0c57d6d57fca59d0517038c968a1b831b071fa67999153000",
			expectErr: false,
		},
		{
			name: "Invalid primary filler length",
			input: &Index{
				Timestamp:       time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "MMM",
				DataType:        "Stat/2.0.0",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "00",
			},
			expected:  "",
			expectErr: true,
		},
		{
			name: "Invalid secondary filler length",
			input: &Index{
				Timestamp:       time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "MM",
				DataType:        "Stat/2.0.0",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "000",
			},
			expected:  "",
			expectErr: true,
		},
		{
			name: "DataType needs padding",
			input: &Index{
				Timestamp:       time.Date(2020, 6, 2, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "MM",
				DataType:        "Stat",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "00",
			},
			expected:  "799397MM000000Statc57d6d57fca59d0517038c968a1b831b071fa67900153000",
			expectErr: false,
		},
		{
			name: "DataType too long",
			input: &Index{
				Timestamp:       time.Date(2020, 6, 2, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "MM",
				DataType:        "Stat/2.0.0.0.0.0",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "00",
			},
			expected:  "",
			expectErr: true,
		},
		{
			name: "Invalid date part",
			input: &Index{
				Timestamp:       time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
				PrimaryFiller:   "MM",
				DataType:        "Stat",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "00",
			},
			expected:  "",
			expectErr: true,
		},
		{
			name:      "Nil index",
			input:     nil,
			expected:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := EncodeIndex(tt.input)
			if (err != nil) != tt.expectErr {
				t.Fatalf("EncodeIndex() error = %v, expectErr %v", err, tt.expectErr)
			}
			if result != tt.expected {
				t.Fatalf("EncodeIndex() result = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestDecodeIndex(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  Index
		expectErr bool
	}{
		{
			name:  "Valid index decoding",
			input: "759388MMStat/2.0.0c57d6d57fca59d0517038c968a1b831b071fa67900153000",
			expected: Index{
				Timestamp:       time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "MM",
				DataType:        "Stat/2.0.0",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "00",
			},
			expectErr: false,
		},
		{
			name:      "Short index string",
			input:     "759388MMStat/2",
			expected:  Index{},
			expectErr: true,
		},
		{
			name:  "DataType needs trimming",
			input: "759388MM000000Statc57d6d57fca59d0517038c968a1b831b071fa67900153000",
			expected: Index{
				Timestamp:       time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "MM",
				DataType:        "Stat",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				SecondaryFiller: "00",
			},
			expectErr: false,
		},
		{
			name:      "Empty secondary filler",
			input:     "759388MMStat/2.0.0c57d6d57fca59d0517038c968a1b831b071fa679150000",
			expectErr: true,
		},
		{
			name:      "Invalid month",
			input:     "123426MMStat/2.0.0c57d6d57fca59d0517038c968a1b831b071fa67900153000",
			expectErr: true,
		},
		{
			name:      "Invalid day",
			input:     "759201MMStat/2.0.0c57d6d57fca59d0517038c968a1b831b071fa67900153000",
			expectErr: true,
		},
		{
			name:      "Invalid time part",
			input:     "759388MMStat/2.0.0c57d6d57fca59d0517038c968a1b831b071fa67900453000",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DecodeIndex(tt.input)
			if (err != nil) != tt.expectErr {
				t.Fatalf("DecodeIndex() error = %v, expectErr %v", err, tt.expectErr)
			}
			if err == nil && !compareIndices(result, &tt.expected) {
				t.Fatalf("DecodeIndex() result = %+v, expected %+v", result, tt.expected)
			}
		})
	}
}

// compareIndices compares two Index structs.
func compareIndices(a, b *Index) bool {
	return a.Timestamp.Equal(b.Timestamp) &&
		a.PrimaryFiller == b.PrimaryFiller &&
		strings.TrimSpace(a.DataType) == strings.TrimSpace(b.DataType) &&
		a.Address == b.Address &&
		a.SecondaryFiller == b.SecondaryFiller
}
