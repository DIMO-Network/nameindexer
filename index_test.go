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
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "",
				DataType:      "Stat/2.0.0",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
				SecondaryFiller: "",
			},
			expected:  "759388MMStat/2.0.0C57D6D57fcA59d0517038c968A1b831B071FA67900153000",
			expectErr: false,
		},
		{
			name: "Valid index encoding with custom fillers",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "XX",
				DataType:      "Stat/2.0.0",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
				SecondaryFiller: "99",
			},
			expected:  "759388XXStat/2.0.0C57D6D57fcA59d0517038c968A1b831B071FA67999153000",
			expectErr: false,
		},
		{
			name: "Valid index encoding with TokenID",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat/2.0.0",
				Subject: Subject{
					TokenID: ref(uint32(1234567890)),
				},
				SecondaryFiller: "00",
			},
			expected:  "759388MMStat/2.0.0T00000000000000000000000000000123456789000153000",
			expectErr: false,
		},
		{
			name: "Valid index encoding with TokenID && Address",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat/2.0.0",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
					TokenID: ref(uint32(1234567890)),
				},
				SecondaryFiller: "00",
			},
			expected:  "759388MMStat/2.0.0C57D6D57fcA59d0517038c968A1b831B071FA67900153000",
			expectErr: false,
		},
		{
			name: "Invalid primary filler length",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MMM",
				DataType:      "Stat/2.0.0",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
				SecondaryFiller: "00",
			},
			expected:  "",
			expectErr: true,
		},
		{
			name: "Invalid secondary filler length",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat/2.0.0",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
				SecondaryFiller: "000",
			},
			expected:  "",
			expectErr: true,
		},
		{
			name: "DataType needs padding",
			input: &Index{
				Timestamp:     time.Date(2020, 6, 2, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
				SecondaryFiller: "00",
			},
			expected:  "799397MM000000StatC57D6D57fcA59d0517038c968A1b831B071FA67900153000",
			expectErr: false,
		},
		{
			name: "DataType too long",
			input: &Index{
				Timestamp:     time.Date(2020, 6, 2, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat/2.0.0.0.0.0",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
				SecondaryFiller: "00",
			},
			expected:  "",
			expectErr: true,
		},
		{
			name: "Invalid date part",
			input: &Index{
				Timestamp:     time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
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
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat/2.0.0",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
				SecondaryFiller: "00",
			},
			expectErr: false,
		},
		{
			name:  "Valid index decoding with tokenId",
			input: "759388MMStat/2.0.0T00000000000000000000000000000000092342300153000",
			expected: Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat/2.0.0",
				Subject: Subject{
					TokenID: ref(uint32(923423)),
				},
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
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat",
				Subject: Subject{
					Address: ref(common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679")),
				},
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
			if tt.expectErr {
				if err == nil {
					t.Fatalf("DecodeIndex() error = nil, expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeIndex() error = %v, expectErr %v", err, tt.expectErr)
			}
			err = SetDefaultsAndValidateIndex(&tt.expected)
			if err != nil {
				t.Fatalf("SetDefaultsAndValidateIndex() error = %v", err)
			}
			if err == nil && !compareIndices(result, &tt.expected) {
				t.Fatalf("DecodeIndex() result = %+v, expected %+v", result, tt.expected)
			}
		})
	}
}

// compareIndices compares two Index structs.
func compareIndices(a, b *Index) bool {
	if a == nil || b == nil {
		return a == b
	}

	if a.Subject.Address != nil && b.Subject.Address != nil {
		if *a.Subject.Address != *b.Subject.Address {
			return false
		}
	} else if a.Subject.TokenID == b.Subject.TokenID {
		return false
	}

	if a.Subject.TokenID != nil && b.Subject.TokenID != nil {
		if *a.Subject.TokenID != *b.Subject.TokenID {
			return false
		}
	} else if a.Subject.TokenID != b.Subject.TokenID {
		return false
	}

	return a.Timestamp.Equal(b.Timestamp) &&
		a.PrimaryFiller == b.PrimaryFiller &&
		strings.TrimSpace(a.DataType) == strings.TrimSpace(b.DataType) &&
		a.SecondaryFiller == b.SecondaryFiller
}

// ref returns a reference to the input.
func ref[T any](v T) *T {
	return &v
}
