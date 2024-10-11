// Package nameindexer provides utilities for creating, decoding, storing, and searching indexable names.
package nameindexer

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/ethereum/go-ethereum/common"
)

const (
	// DateLength is the length of the date part of an index string.
	DateLength = 6
	// FillerLength is the length of the filler parts of an index string.
	FillerLength = 2
	// DataTypeLength is the length of the data type part of an index string.
	DataTypeLength = 20
	// DIDLength is the length of the DID part of an index string.
	DIDLength = 64
	// TimeLength is the length of the time part of an index string.
	TimeLength = 6
	// AddressLength is the length of an ethereum address part of an index string.
	AddressLength = 40

	// TotalLength is the total length of an index string.
	TotalLength = DIDLength + DateLength + FillerLength + DataTypeLength + TimeLength + AddressLength + DIDLength + FillerLength

	// DateMax is the maximum value used for date calculations in the index.
	DateMax = 999999
	// HhmmssFormat is the time format used in the index string.
	HhmmssFormat = "150405"

	// DefaultPrimaryFiller is the default filler value between the date and data type.
	DefaultPrimaryFiller = "MM"
	// DefaultSecondaryFiller is the default filler value between the subject and time.
	DefaultSecondaryFiller = "00"

	DataTypePadding = "!"
)

// digitRegex is a regular expression for matching digits.
var digitRegex = regexp.MustCompile(`^\d+$`)

// InvalidError represents an error type for invalid arguments.
type InvalidError string

// Error implements the error interface for InvalidError.
func (e InvalidError) Error() string {
	return "invalid index " + string(e)
}

// Index represents the components of a decoded index.
type Index struct {
	// Subject is the subject of the data represented by the index.
	Subject cloudevent.NFTDID `json:"subject"`
	// Timestamp is the full timestamp used for date and time.
	Timestamp time.Time `json:"timestamp"`
	// PrimaryFiller is the filler value between the date and data type, typically "MM". If empty, defaults to "MM".
	PrimaryFiller string `json:"primaryFiller"`
	// DataType is the type of data, left-padded with zeros or truncated to 10 characters.
	DataType string `json:"dataType"`
	// Source is the source of the data represented by the index.
	Source common.Address `json:"source"`
	// Producer is the producer of the data represented by the index.
	Producer cloudevent.NFTDID `json:"producer"`
	// SecondaryFiller is the filler value between the subject and time, typically "00". If empty, defaults to "00".
	SecondaryFiller string `json:"secondaryFiller"`
	// Optional data for additional metadata
	Optional string `json:"optional"`
}

// EncodeIndex creates an indexable name string from the Index struct.
// This function will modify the index to have correctly padded values.
// The index string format is:
//
//	subject + date + time + primaryFiller + source + dataType + secondaryFiller + producer + optional
//
// where:
//   - subject is the NFTDID of the data's subject
//     -- chainId + contractAddress + tokenID
//     -- chainId is a 16-character hexadecimal string representing the uint64 chain ID
//     -- contractAddress is a 40-character hexadecimal string representing the contract address
//     -- tokenID is an 8-character hexadecimal string representing the uint32 token ID
//   - date is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
//   - time is the time in UTC in the format HHMMSS
//   - primaryFiller is a constant string of length 2
//   - source is a 40-character hexadecimal string representing the source address
//   - dataType is the data type left-padded with `!` or truncated to 20 characters
//   - secondaryFiller is a constant string of length 2
//   - producer is the NFTDID of the data's producer
//     -- chainId + contractAddress + tokenID
//     -- chainId is a 16-character hexadecimal string representing the uint64 chain ID
//     -- contractAddress is a 40-character hexadecimal string representing the contract address
//     -- tokenID is an 8-character hexadecimal string representing the uint32 token ID
//   - optional is an optional string that can be appended to the index
func EncodeIndex(origIndex *Index) (string, error) {
	index := WithDefaults(origIndex)

	err := ValidateIndex(index)
	if err != nil {
		return "", err
	}

	yymmddInt := (index.Timestamp.Year()%100)*10000 + int(index.Timestamp.Month())*100 + index.Timestamp.Day()
	datePart := DateMax - yymmddInt

	// Format time part
	timePart := index.Timestamp.UTC().Format(HhmmssFormat)
	subject, err := EncodeNFTDID(index.Subject)
	if err != nil {
		return "", fmt.Errorf("subject: %w", err)
	}
	producer, err := EncodeNFTDID(index.Producer)
	if err != nil {
		return "", fmt.Errorf("producer: %w", err)
	}

	unPrefixedSource := EncodeAddress(index.Source)
	dataType := EncodeDataType(index.DataType)
	// Construct the index string
	encodedIndex := fmt.Sprintf(
		"%s%06d%s%s%s%s%s%s%s",
		subject,
		datePart,
		timePart,
		index.PrimaryFiller,
		unPrefixedSource,
		dataType,
		index.SecondaryFiller,
		producer,
		index.Optional,
	)

	return encodedIndex, nil
}

// DecodeIndex decodes an index string into its constituent parts.
// It returns an Index struct containing the decoded components.
// The index string format is expected to be:
//
//	subject + date + time + primaryFiller + source + dataType + secondaryFiller  + producer + optional
//
// where:
//   - subject is the NFTDID of the data's subject
//     -- chainId + contractAddress + tokenID
//     -- chainId is a 16-character hexadecimal string representing the uint64 chain ID
//     -- contractAddress is a 40-character hexadecimal string representing the contract address
//     -- tokenID is an 8-character hexadecimal string representing the uint32 token ID
//   - date is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
//   - time is the time in UTC in the format HHMMSS
//   - primaryFiller is a constant string of length 2
//   - source is a 40-character hexadecimal string representing the source address
//   - dataType is the data type left-padded with `!` or truncated to 20 characters
//   - secondaryFiller is a constant string of length 2
//   - producer is the NFTDID of the data's producer
//     -- chainId + contractAddress + tokenID
//     -- chainId is a 16-character hexadecimal string representing the uint64 chain ID
//     -- contractAddress is a 40-character hexadecimal string representing the contract address
//     -- tokenID is an 8-character hexadecimal string representing the uint32 token ID
//   - optional is an optional string that can be appended to the index
func DecodeIndex(index string) (*Index, error) {
	if len(index) < TotalLength {
		return nil, InvalidError(fmt.Sprintf("length %d is less than %d", len(index), TotalLength))
	}

	var start int
	subjectPart, start := getNextPart(index, start, DIDLength)
	datePart, start := getNextPart(index, start, DateLength)
	timePart, start := getNextPart(index, start, TimeLength)
	primaryFillerPart, start := getNextPart(index, start, FillerLength)
	sourcePart, start := getNextPart(index, start, AddressLength)
	dataTypePart, start := getNextPart(index, start, DataTypeLength)
	secondaryFillerPart, start := getNextPart(index, start, FillerLength)
	producerPart, start := getNextPart(index, start, DIDLength)

	// put the rest of the index into optional
	optional := index[start:]

	// Decode date
	dateInt, err := strconv.Atoi(datePart)
	if err != nil {
		return nil, fmt.Errorf("date part: %w", err)
	}
	yymmddInt := DateMax - dateInt

	year := (yymmddInt / 10000) + 2000
	month := (yymmddInt % 10000) / 100
	day := yymmddInt % 100

	if month < 1 || month > 12 {
		return nil, InvalidError("month out of range")
	}
	if day < 1 || day > 31 {
		return nil, InvalidError("day out of range")
	}

	subject, err := DecodeNFTDIDIndex(subjectPart)
	if err != nil {
		return nil, fmt.Errorf("subject part: %w", err)
	}

	// Decode time
	ts, err := time.Parse(HhmmssFormat, timePart)
	if err != nil {
		return nil, fmt.Errorf("time part: %w", err)
	}
	fullTime := time.Date(year, time.Month(month), day, ts.Hour(), ts.Minute(), ts.Second(), 0, time.UTC)

	if !common.IsHexAddress(sourcePart) {
		return nil, InvalidError("source is not a valid address")
	}

	producer, err := DecodeNFTDIDIndex(producerPart)
	if err != nil {
		return nil, fmt.Errorf("producer part: %w", err)
	}

	decodedIndex := &Index{
		Subject:         subject,
		Timestamp:       fullTime,
		PrimaryFiller:   primaryFillerPart,
		Source:          common.HexToAddress(sourcePart),
		DataType:        DecodeDataType(dataTypePart),
		Producer:        producer,
		SecondaryFiller: secondaryFillerPart,
		Optional:        optional,
	}

	return decodedIndex, nil
}

func getNextPart(encodedIndex string, start, offset int) (string, int) {
	end := start + offset
	value := encodedIndex[start:end]
	nextStart := end
	return value, nextStart
}

// ValidateIndex validates the index.
func ValidateIndex(index *Index) error {
	if index == nil {
		return InvalidError("nil index")
	}

	// Validate primary filler length
	if len(index.PrimaryFiller) != FillerLength {
		return InvalidError("primary filler length")
	}
	// Validate secondary filler length
	if len(index.SecondaryFiller) != FillerLength {
		return InvalidError("secondary filler length")
	}

	// Format date part
	if index.Timestamp.IsZero() || index.Timestamp.Year() < 2000 || index.Timestamp.Year() > 2099 {
		return InvalidError("timestamp year must be between 2000 and 2099")
	}

	return nil
}

// WithDefaults sets default values for the index and santizes the data type.
func WithDefaults(index *Index) *Index {
	if index == nil {
		return nil
	}

	retIndex := *index

	if retIndex.PrimaryFiller == "" {
		retIndex.PrimaryFiller = DefaultPrimaryFiller
	}
	if retIndex.SecondaryFiller == "" {
		retIndex.SecondaryFiller = DefaultSecondaryFiller
	}
	return &retIndex
}

// EncodeDataType pads the data type with `*` if shorter than required.
// It truncates the data type if longer than required.
func EncodeDataType(dataType string) string {
	// Validate data type length
	if len(dataType) > DataTypeLength {
		// truncate data type if longer than required
		return dataType[:DataTypeLength]
	}
	// Pad data type with zeros if shorter than required
	if len(dataType) < DataTypeLength {
		return fmt.Sprintf("%s%s", strings.Repeat(DataTypePadding, DataTypeLength-len(dataType)), dataType)
	}
	return dataType
}

// EncodeAddress encodes an ethereum address without the 0x prefix.
func EncodeAddress(address common.Address) string {
	return address.Hex()[2:]
}

// DecodeDataType decodes a data type string by removing padding.
func DecodeDataType(dataType string) string {
	return strings.TrimLeft(dataType, DataTypePadding)
}

// EncodeNFTDID encodes an NFTDID struct into an indexable string.
// This format is different from the standard NFTDID.
func EncodeNFTDID(did cloudevent.NFTDID) (string, error) {
	if !common.IsHexAddress(did.ContractAddress.Hex()) {
		return "", InvalidError("contract address is not a valid address")
	}
	unPrefixedAddr := EncodeAddress(did.ContractAddress)
	return fmt.Sprintf("%016x%s%08x", did.ChainID, unPrefixedAddr, did.TokenID), nil
}

// DecodeNFTDIDIndex decodes an NFTDID string into a cloudevent.NFTDID struct.
func DecodeNFTDIDIndex(indexNFTDID string) (cloudevent.NFTDID, error) {
	if len(indexNFTDID) != DIDLength {
		return cloudevent.NFTDID{}, InvalidError("invalid NFTDID length")
	}
	var start int
	chainIDPart, start := getNextPart(indexNFTDID, start, 16)
	chainID, err := strconv.ParseUint(chainIDPart, 16, 64)
	if err != nil {
		return cloudevent.NFTDID{}, fmt.Errorf("chain ID: %w", err)
	}

	contractPart, start := getNextPart(indexNFTDID, start, AddressLength)
	if !common.IsHexAddress(contractPart) {
		return cloudevent.NFTDID{}, InvalidError("contract address is not a valid address")
	}
	contractAddress := common.HexToAddress(contractPart)

	tokenIDPart, _ := getNextPart(indexNFTDID, start, 8)
	tokenID, err := strconv.ParseUint(tokenIDPart, 16, 32)
	if err != nil {
		return cloudevent.NFTDID{}, fmt.Errorf("token ID: %w", err)
	}

	return cloudevent.NFTDID{
		ChainID:         chainID,
		ContractAddress: contractAddress,
		TokenID:         uint32(tokenID),
	}, nil
}
