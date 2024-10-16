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
		input     *CloudEventIndex
		expected  string
		expectErr bool
	}{
		{
			name: "Valid index encoding with default fillers",
			input: &CloudEventIndex{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "",
				DataType:      "Stat_2.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "MM" +
				"6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" +
				"00" + strings.Repeat("0", 64),
			expectErr: false,
		},
		{
			name: "Valid index encoding with custom fillers",
			input: &CloudEventIndex{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "XX",
				DataType:      "Stat_2.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "99",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
				Producer: cloudevent.NFTDID{
					ChainID:         153,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         3649,
				},
			},
			expected: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "XX" +
				"6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" +
				"99" + "0000000000000099" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + "00000e41",
			expectErr: false,
		},
		{
			name: "DataType needs padding",
			input: &CloudEventIndex{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "MM" +
				"6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 16) +
				"Stat" + "00" + strings.Repeat("0", 64),
			expectErr: false,
		},
		{
			name: "Invalid primary filler length",
			input: &CloudEventIndex{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MMM",
				DataType:      "Stat_2.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "MM" +
				"6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" +
				"00" + strings.Repeat("0", 64),
			expectErr: false,
		},
		{
			name: "Invalid secondary filler length",
			input: &CloudEventIndex{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat_2.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "000",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "MM" +
				"6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" +
				"00" + strings.Repeat("0", 64),
			expectErr: false,
		},
		{
			name: "DataType too long",
			input: &CloudEventIndex{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "SuperDuperDataValidForEveryone/2.0.0.0.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
				Producer: cloudevent.NFTDID{
					ChainID:         0,
					ContractAddress: common.HexToAddress(strings.Repeat("0", 40)),
					TokenID:         0,
				},
			},
			expected: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "MM" +
				"6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + "SuperDuperDataValidF" +
				"00" + strings.Repeat("0", 64),
			expectErr: false,
		},
		{
			name: "Invalid date part",
			input: &CloudEventIndex{
				Timestamp:     time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
				PrimaryFiller: "MM",
				DataType:      "Stat",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "00",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
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
			result, err := EncodeCloudEventIndex(tt.input)
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
		expected  CloudEventIndex
		expectErr bool
	}{
		{
			name: "Valid index decoding",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"0000000f" + "759388" + "153000" + "MM" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				strings.Repeat("0", 15) + "8" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + strings.Repeat("0", 7) + "7",
			expected: CloudEventIndex{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "",
				DataType:      "Stat_2.0.0",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         15,
				},
				SecondaryFiller: "",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
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
				"99999999" + "123426" + "153000" + "MM" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Invalid day",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"99999999" + "759201" + "153000" + "MM" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Short index string",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "MM" + "0000000000Stat_2",
			expected:  CloudEventIndex{},
			expectErr: true,
		},
		{
			name: "DataType needs trimming",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "MM" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 16) + "Stat" + "00" +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expected: CloudEventIndex{
				Timestamp:     time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller: "",
				DataType:      "Stat",
				Subject: cloudevent.NFTDID{
					ChainID:         1,
					ContractAddress: common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"),
					TokenID:         1,
				},
				SecondaryFiller: "",
				Source:          common.HexToAddress("0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0"),
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
				"00000001" + "759388" + "153000" + "MM" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Invalid month",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"123426" + "153000" + "MM" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Invalid day",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"759201" + "153000" + "MM" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
		{
			name: "Invalid time part",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"759388" + "00453000" + "MM" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				"0000000000000000000000000000000000000000000000000000000000000000",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DecodeCloudEventIndex(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("DecodeIndex() error = nil, expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeIndex() error = %v, expectErr %v", err, tt.expectErr)
			}

			if !reflect.DeepEqual(*result, tt.expected) {
				t.Fatalf("DecodeIndex() result = %+v, expected %+v", *result, tt.expected)
			}
		})
	}
}
func TestEncodeCloudEvent(t *testing.T) {
	tests := []struct {
		name            string
		cloudEvent      *cloudevent.CloudEventHeader
		secondaryFiller string
		expected        string
		expectErr       bool
	}{
		{
			name: "Valid cloud event encoding",
			cloudEvent: &cloudevent.CloudEventHeader{
				SpecVersion: "1.0",
				Subject:     "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_15",
				Time:        time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				Type:        "dimo.status",
				DataVersion: "Stat_2.0.0",
				Producer:    "did:nft:0:0x0000000000000000000000000000000000000000_0",
				Source:      "0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0",
			},
			secondaryFiller: "",
			expected: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"0000000f" + "759388" + "153000" + "MA" +
				"6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" +
				"00" + strings.Repeat("0", 64),
			expectErr: false,
		},
		{
			name: "Invalid subject DID",
			cloudEvent: &cloudevent.CloudEventHeader{
				SpecVersion: "1.0",
				Subject:     "invalid-did",
				Time:        time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				Type:        "dimo.status",
				DataVersion: "Stat_2.0.0",
				Producer:    "did:nft:0:0x0000000000000000000000000000000000000000_0",
				Source:      "0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0",
			},
			secondaryFiller: "00",
			expected:        "",
			expectErr:       true,
		},
		{
			name: "Invalid producer DID",
			cloudEvent: &cloudevent.CloudEventHeader{
				SpecVersion: "1.0",
				Subject:     "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1",
				Time:        time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				Type:        "dimo.status",
				DataVersion: "Stat_2.0.0",
				Producer:    "invalid-did",
				Source:      "0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0",
			},
			secondaryFiller: "00",
			expected:        "",
			expectErr:       true,
		},
		{
			name: "Invalid source address",
			cloudEvent: &cloudevent.CloudEventHeader{
				SpecVersion: "1.0",
				Subject:     "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1",
				Time:        time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				Type:        "dimo.status",
				DataVersion: "Stat_2.0.0",
				Producer:    "did:nft:0:0x0000000000000000000000000000000000000000_0",
				Source:      "invalid-address",
			},
			secondaryFiller: "00",
			expected:        "",
			expectErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := EncodeCloudEvent(tt.cloudEvent, tt.secondaryFiller)
			if (err != nil) != tt.expectErr {
				t.Fatalf("EncodeCloudEvent() error = %v, expectErr %v", err, tt.expectErr)
			}
			if result != tt.expected {
				t.Fatalf("EncodeCloudEvent() result = %v, expected %v", result, tt.expected)
			}
		})
	}
}
func TestDecodeCloudEvent(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  *cloudevent.CloudEventHeader
		expectErr bool
	}{
		{
			name: "Valid cloud event decoding",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "MA" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				strings.Repeat("0", 15) + "8" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + strings.Repeat("0", 7) + "7",
			expected: &cloudevent.CloudEventHeader{
				SpecVersion: "1.0",
				Subject:     "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1",
				Time:        time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				Type:        "dimo.status",
				DataVersion: "Stat_2.0.0",
				Producer:    "did:nft:8:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_7",
				Source:      "0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0",
			},
			expectErr: false,
		},
		{
			name: "Invalid index string",
			input: strings.Repeat("0", 15) + "1" + "invalid" +
				"00000001" + "759388" + "153000" + "MA" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				strings.Repeat("0", 15) + "8" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + strings.Repeat("0", 7) + "7",
			expected:  nil,
			expectErr: true,
		},
		{
			name: "Invalid source address",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "MA" + "invalid" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				strings.Repeat("0", 15) + "8" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + strings.Repeat("0", 7) + "7",
			expected:  nil,
			expectErr: true,
		},
		{
			name: "Invalid primary filler",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"00000001" + "759388" + "153000" + "XX" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				strings.Repeat("0", 15) + "8" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + strings.Repeat("0", 7) + "7",
			expected: &cloudevent.CloudEventHeader{
				SpecVersion: "1.0",
				Subject:     "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1",
				Time:        time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				Type:        "dimo.unknown",
				DataVersion: "Stat_2.0.0",
				Producer:    "did:nft:8:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_7",
				Source:      "0x6C7cFb99AcfEFbA12DeD34387c11697061C196d0",
			},
			expectErr: false,
		},
		{
			name: "Invalid timestamp",
			input: strings.Repeat("0", 15) + "1" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" +
				"99999999" + "123426" + "153000" + "MA" + "6C7cFb99AcfEFbA12DeD34387c11697061C196d0" + strings.Repeat(DataTypePadding, 10) + "Stat_2.0.0" + "00" +
				strings.Repeat("0", 15) + "8" + "bA5738a18d83D41847dfFbDC6101d37C69c9B0cF" + strings.Repeat("0", 7) + "7",
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, secondaryFiller, err := DecodeCloudEvent(tt.input)
			if (err != nil) != tt.expectErr {
				t.Fatalf("DecodeCloudEvent() error = %v, expectErr %v", err, tt.expectErr)
			}
			if !tt.expectErr && result != nil {
				if !reflect.DeepEqual(result, tt.expected) {
					t.Fatalf("DecodeCloudEvent() result = %+v, expected %+v", result, tt.expected)
				}
				if secondaryFiller != "" {
					t.Fatalf("DecodeCloudEvent() secondaryFiller = %v, expected %v", secondaryFiller, "00")
				}
			}
		})
	}
}
