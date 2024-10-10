package nameindexer

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
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
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected:  strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + "00000001" + "759388" + "MM" + "0000000000Stat/2.0.0" + "00" + "153000" + strings.Repeat("0", 40) + "0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: false,
		},
		{
			name: "Valid index encoding with custom fillers",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "XX",
				DataType:      "Stat/2.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "99",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected:  strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + "00000001" + "759388" + "XX" + "0000000000Stat/2.0.0" + "99" + "153000" + strings.Repeat("0", 40) + "0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: false,
		},
		{
			name: "DataType needs padding",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected:  strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + "00000001" + "759388" + "MM" + strings.Repeat("0", 16) + "Stat" + "00" + "153000" + strings.Repeat("0", 40) + "0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: false,
		},
		{
			name: "Invalid primary filler length",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MMM",
				DataType:      "Stat/2.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
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
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "000",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected:  "",
			expectErr: true,
		},
		{
			name: "DataType too long",
			input: &Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "SuperDuperDataValidForEveryone/2.0.0.0.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected:  strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + "00000001" + "759388" + "MM" + "SuperDuperDataValidF" + "00" + "153000" + strings.Repeat("0", 40) + strings.Repeat("0", 64),
			expectErr: false,
		},
		{
			name: "Invalid date part",
			input: &Index{
				Timestamp:     time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
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

// 0000000000000001C57D6D57fcA59d0517038c968A1b831B071FA67900000001759388XX0000000000Stat/2.0.09915300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
// 0000000000000001c57d6d57fca59d0517038c968a1b831b071fa67900000001759388XX0000000000Stat/2.0.09915300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
func TestDecodeIndex(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  Index
		expectErr bool
	}{
		{
			name: "Valid index decoding",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "MM" + "0000000000Stat/2.0.0" + "00" + "153000" + strings.Repeat("0", 40) +
				strings.Repeat("0", 15) + "8" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + strings.Repeat("0", 7) + "7",
			expected: Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "0000000000Stat/2.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         8,
					ContractAddress: common.HexToAddress("bA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         7,
				},
			},
			expectErr: false,
		},
		{
			name: "Invalid month",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"99999999" + "123426" + "MM" + "0000000000Stat/2.0.0" + "00" + "153000" + strings.Repeat("0", 40) +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Invalid day",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"99999999" + "759201" + "MM" + "0000000000Stat/2.0.0" + "00" + "153000" + strings.Repeat("0", 40) +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Short index string",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "MM" + "0000000000Stat/2",
			expected:  Index{},
			expectErr: true,
		},
		{
			name: "DataType needs trimming",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "MM" + "0000000000000000Stat" + "00" + "153000" + strings.Repeat("0", 40) +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expected: Index{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "0000000000000000Stat",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress(strings.Repeat("0", 40)),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expectErr: false,
		},
		{
			name: "Empty secondary filler",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "MM" + "0000000000Stat/2.0.0" + "153000" + strings.Repeat("0", 40) +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Invalid month",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"123426" + "MM" + "0000000000Stat/2.0.0" + "00" + "153000" + strings.Repeat("0", 40) +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Invalid day",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"759201" + "MM" + "0000000000Stat/2.0.0" + "00" + "153000" + strings.Repeat("0", 40) +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Invalid time part",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"759388" + "MM" + "0000000000Stat/2.0.0" + "00" + "00453000" + strings.Repeat("0", 40) +
				"0000000000000000000000000000000000000000000000000000000000000000",
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
			index, err := SetDefaultsAndValidateIndex(&tt.expected)
			if err != nil {
				t.Fatalf("SetDefaultsAndValidateIndex() error = %v", err)
			}
			if !reflect.DeepEqual(*result, *index) {
				t.Fatalf("DecodeIndex() result = %+v, expected %+v", *result, *index)
			}
		})
	}
}

func compareIndices(a, b *Index) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.Subject.ContractAddress.Hex() == b.Subject.ContractAddress.Hex() && a.Timestamp.Equal(b.Timestamp) &&
		a.PrimaryFiller == b.PrimaryFiller &&
		strings.TrimSpace(a.DataType) == strings.TrimSpace(b.DataType) &&
		a.SecondaryFiller == b.SecondaryFiller && a.Subject == b.Subject && a.Producer == b.Producer && a.Source == b.Source
}
