// Package nameindexer provides utilities for creating, decoding, storing, and searching indexable names.
package nameindexer

import (
	"fmt"
	"regexp"
	"strconv"
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
	// totallength actual value is 140.
	TotalLength = DIDLength + DateLength + FillerLength + DataTypeLength + TimeLength + AddressLength + DIDLength + FillerLength

	// DateMax is the maximum value used for date calculations in the index.
	DateMax = 999999
	// HhmmssFormat is the time format used in the index string.
	HhmmssFormat = "150405"

	// DefaultPrimaryFiller is the default filler value between the date and data type.
	DefaultPrimaryFiller = "MM"
	// DefaultSecondaryFiller is the default filler value between the subject and time.
	DefaultSecondaryFiller = "00"
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
	Subject cloudevent.NFTDID
	// Timestamp is the full timestamp used for date and time.
	Timestamp time.Time
	// PrimaryFiller is the filler value between the date and data type, typically "MM". If empty, defaults to "MM".
	PrimaryFiller string
	// DataType is the type of data, left-padded with zeros or truncated to 10 characters.
	DataType string
	// Source is the source of the data represented by the index.
	Source common.Address
	// Producer is the producer of the data represented by the index.
	Producer cloudevent.NFTDID
	// SecondaryFiller is the filler value between the subject and time, typically "00". If empty, defaults to "00".
	SecondaryFiller string
}

// EncodeIndex creates an indexable name string from the Index struct.
// This function will modify the index to have correctly padded values.
// The index string format is:
//
//	subject + date + primaryFiller + dataType + time + secondaryFiller + source + producer
//
// where:
//   - subject is the NFTDID of the data's subject
//     -- chainId + contractAddress + tokenID
//     -- chainId is a 16-character hexadecimal string representing the uint64 chain ID
//     -- contractAddress is a 40-character hexadecimal string representing the contract address
//     -- tokenID is an 8-character hexadecimal string representing the uint32 token ID
//   - date is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
//   - primaryFiller is a constant string of length 2
//   - dataType is the data type left-padded with zeros or truncated to 20 characters
//   - secondaryFiller is a constant string of length 2
//   - time is the time in UTC in the format HHMMSS
//   - source is a 40-character hexadecimal string representing the source address
//   - producer is the NFTDID of the data's producer
//     -- chainId + contractAddress + tokenID
//     -- chainId is a 16-character hexadecimal string representing the uint64 chain ID
//     -- contractAddress is a 40-character hexadecimal string representing the contract address
//     -- tokenID is an 8-character hexadecimal string representing the uint32 token ID
func EncodeIndex(origIndex *Index) (string, error) {
	index, err := SetDefaultsAndValidateIndex(origIndex)
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
	// Construct the index string
	encodedIndex := fmt.Sprintf(
		"%s%06d%s%s%s%s%s%s",
		subject,
		datePart,
		index.PrimaryFiller,
		index.DataType,
		index.SecondaryFiller,
		timePart,
		unPrefixedSource,
		producer,
	)

	return encodedIndex, nil
}

// DecodeIndex decodes an index string into its constituent parts.
// It returns an Index struct containing the decoded components.
// The index string format is expected to be:
//
//	date + primaryFiller + dataType + subject + secondaryFiller + time
//
// where:
//   - date is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
//   - primaryFiller is a constant string of length 2
//   - dataType is the data type left-padded with zeros or truncated to 10 characters
//   - subject is the hexadecimal representation of the device's address or the token ID prefixed with "T"
//   - secondaryFiller is a constant string of length 2
//   - time is the time in UTC in the format HHMMSS
func DecodeIndex(index string) (*Index, error) {
	if len(index) != TotalLength {
		return nil, InvalidError(fmt.Sprintf("length %d is not %d", len(index), TotalLength))
	}

	var start int
	subjectPart, start := getNextPart(index, start, DIDLength)
	datePart, start := getNextPart(index, start, DateLength)
	primaryFillerPart, start := getNextPart(index, start, FillerLength)
	dataTypePart, start := getNextPart(index, start, DataTypeLength)
	secondaryFillerPart, start := getNextPart(index, start, FillerLength)
	timePart, start := getNextPart(index, start, TimeLength)
	sourcePart, start := getNextPart(index, start, AddressLength)
	producerPart, _ := getNextPart(index, start, DIDLength)

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
		DataType:        dataTypePart,
		Source:          common.HexToAddress(sourcePart),
		Producer:        producer,
		SecondaryFiller: secondaryFillerPart,
	}

	return decodedIndex, nil
}

func getNextPart(encodedIndex string, start, offset int) (string, int) {
	end := start + offset
	value := encodedIndex[start:end]
	nextStart := end
	return value, nextStart
}

// SetDefaultsAndValidateIndex sets default values for empty fields and validates the index.
func SetDefaultsAndValidateIndex(index *Index) (*Index, error) {
	if index == nil {
		return nil, InvalidError("nil index")
	}
	retIndex := *index
	// Set default fillers if empty
	if retIndex.PrimaryFiller == "" {
		retIndex.PrimaryFiller = DefaultPrimaryFiller
	}
	if retIndex.SecondaryFiller == "" {
		retIndex.SecondaryFiller = DefaultSecondaryFiller
	}

	// Validate primary filler length
	if len(retIndex.PrimaryFiller) != FillerLength {
		return nil, InvalidError("primary filler length")
	}
	// Validate secondary filler length
	if len(retIndex.SecondaryFiller) != FillerLength {
		return nil, InvalidError("secondary filler length")
	}
	retIndex.DataType = SantatizeDataType(index.DataType)

	// Format date part
	if retIndex.Timestamp.IsZero() || retIndex.Timestamp.Year() < 2000 || retIndex.Timestamp.Year() > 2099 {
		return nil, InvalidError("timestamp year must be between 2000 and 2099")
	}

	return &retIndex, nil
}

// SantatizeDataType pads the data type with zeros if shorter than required.
// It truncates the data type if longer than required.
func SantatizeDataType(dataType string) string {
	// Validate data type length
	if len(dataType) > DataTypeLength {
		// truncate data type if longer than required
		return dataType[:DataTypeLength]
	}
	// Pad data type with zeros if shorter than required
	if len(dataType) < DataTypeLength {
		return fmt.Sprintf("%0*s", DataTypeLength, dataType)
	}
	return dataType
}

func EncodeAddress(address common.Address) string {
	return address.Hex()[2:]
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
