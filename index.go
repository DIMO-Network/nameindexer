// Package nameindexer provides utilities for creating, decoding, storing, and searching indexable names.
package nameindexer

import (
	"fmt"
	"strconv"
	"strings"
	"time"

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

// InvalidError represents an error type for invalid arguments.
type InvalidError string

// Error implements the error interface for InvalidError.
func (e InvalidError) Error() string {
	return "invalid index " + string(e)
}

// Index represents the components of a decoded index.
type Index struct {
	// Subject is the subject of the data represented by the index.
	Subject string
	// Timestamp is the full timestamp used for date and time.
	Timestamp time.Time
	// PrimaryFiller is the filler value between the date and data type, typically "MM". If empty, defaults to "MM".
	PrimaryFiller string
	// DataType is the type of data, left-padded with @ or truncated to 20 characters.
	DataType string
	// Source is the source of the data represented by the index.
	Source string `json:"source"`
	// Producer is the producer of the data represented by the index.
	Producer string `json:"producer"`
	// SecondaryFiller is the filler value between the subject and time, typically "00". If empty, defaults to "00".
	SecondaryFiller string
	// Optional data for additional metadata
	Optional string `json:"optional"`
}

func (i Index) WithEncodedParts() Index {
	return Index{
		Subject:         EncodeSubject(i.Subject),
		Timestamp:       i.Timestamp,
		PrimaryFiller:   EncodePrimaryFiller(i.PrimaryFiller),
		DataType:        EncodeDataType(i.DataType),
		Source:          EncodeSource(common.HexToAddress(i.Source)),
		Producer:        EncodeProducer(i.Producer),
		SecondaryFiller: EncodeSecondaryFiller(i.SecondaryFiller),
		Optional:        i.Optional,
	}
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
	if origIndex == nil {
		return "", InvalidError("index is nil")
	}
	index := origIndex.WithEncodedParts()
	datePart, err := EncodeDate(index.Timestamp)
	if err != nil {
		return "", fmt.Errorf("date part: %w", err)
	}
	timePart := EncodeTime(index.Timestamp)

	// Construct the index string
	encodedIndex :=
		index.Subject +
			datePart +
			timePart +
			index.PrimaryFiller +
			index.Source +
			index.DataType +
			index.SecondaryFiller +
			index.Producer +
			index.Optional

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

	fullTime, err := DecodeDateAndTime(datePart, timePart)
	if err != nil {
		return nil, err
	}

	decodedIndex := &Index{
		Subject:         DecodeSubject(subjectPart),
		Timestamp:       fullTime,
		PrimaryFiller:   DecodePrimaryFiller(primaryFillerPart),
		Source:          DecodeSource(sourcePart),
		DataType:        DecodeDataType(dataTypePart),
		Producer:        DecodeProducer(producerPart),
		SecondaryFiller: DecodeSecondaryFiller(secondaryFillerPart),
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

// DecodeDataType decodes a data type string by removing padding.
func DecodeDataType(dataType string) string {
	return strings.TrimLeft(dataType, DataTypePadding)
}

// EncodePrimaryFiller pads the primary filler with `M` if shorter than required.
// It truncates the primary filler if longer than required.
func EncodePrimaryFiller(primaryFiller string) string {
	// Validate primary filler length
	if len(primaryFiller) > FillerLength {
		// truncate primary filler if longer than required
		return primaryFiller[:FillerLength]
	}
	// Pad primary filler with zeros if shorter than required
	if len(primaryFiller) < FillerLength {
		return fmt.Sprintf("%s%s", strings.Repeat("M", FillerLength-len(primaryFiller)), primaryFiller)
	}
	return primaryFiller
}

// DecodePrimaryFiller decodes a primary filler string by removing padding.
func DecodePrimaryFiller(primaryFiller string) string {
	return strings.TrimLeft(primaryFiller, "M")
}

// EncodeSecondaryFiller pads the secondary filler with `0` if shorter than required.
// It truncates the secondary filler if longer than required.
func EncodeSecondaryFiller(secondaryFiller string) string {
	// Validate secondary filler length
	if len(secondaryFiller) > FillerLength {
		// truncate secondary filler if longer than required
		return secondaryFiller[:FillerLength]
	}
	// Pad secondary filler with zeros if shorter than required
	if len(secondaryFiller) < FillerLength {
		return fmt.Sprintf("%s%s", strings.Repeat("0", FillerLength-len(secondaryFiller)), secondaryFiller)
	}
	return secondaryFiller
}

// DecodeSecondaryFiller decodes a secondary filler string by removing padding.
func DecodeSecondaryFiller(secondaryFiller string) string {
	return strings.TrimLeft(secondaryFiller, "0")
}

// EncodeSource pads the source with `!` if shorter than required.
// It truncates the source if longer than required.
func EncodeSource(source common.Address) string {
	sourceStr := source.Hex()
	// Validate source length
	if len(sourceStr) > AddressLength {
		// truncate source if longer than required
		return sourceStr[:AddressLength]
	}
	// Pad source with zeros if shorter than required
	if len(sourceStr) < AddressLength {
		return fmt.Sprintf("%s%s", strings.Repeat(DataTypePadding, AddressLength-len(sourceStr)), sourceStr)
	}
	return sourceStr
}

// DecodeSource decodes a source string by removing padding
func DecodeSource(source string) string {
	return strings.TrimLeft(source, DataTypePadding)
}

// EncodeSubject pads the subject with `!` if shorter than required.
// It truncates the subject if longer than required.
func EncodeSubject(subject string) string {
	// Validate subject length
	if len(subject) > DIDLength {
		// truncate subject if longer than required
		return subject[:DIDLength]
	}
	// Pad subject with zeros if shorter than required
	if len(subject) < DIDLength {
		return fmt.Sprintf("%s%s", strings.Repeat(DataTypePadding, DIDLength-len(subject)), subject)
	}
	return subject
}

// DecodeSubject decodes a subject string by removing padding.
func DecodeSubject(subject string) string {
	return strings.TrimLeft(subject, DataTypePadding)
}

// EncodeProducer pads the producer with `!` if shorter than required.
// It truncates the producer if longer than required.
func EncodeProducer(producer string) string {
	// Validate producer length
	if len(producer) > DIDLength {
		// truncate producer if longer than required
		return producer[:DIDLength]
	}
	// Pad producer with zeros if shorter than required
	if len(producer) < DIDLength {
		return fmt.Sprintf("%s%s", strings.Repeat(DataTypePadding, DIDLength-len(producer)), producer)
	}
	return producer
}

// DecodeProducer decodes a producer string by removing padding.
func DecodeProducer(producer string) string {
	return strings.TrimLeft(producer, DataTypePadding)
}

// EncodeDate encodes a time.Time into a string.
func EncodeDate(date time.Time) (string, error) {
	if date.IsZero() || date.Year() < 2000 || date.Year() > 2099 {
		return "", InvalidError("timestamp year must be between 2000 and 2099")
	}
	yymmddInt := (date.Year()%100)*10000 + int(date.Month())*100 + date.Day()
	datePart := DateMax - yymmddInt
	return fmt.Sprintf("%06d", datePart), nil
}

// EncodeTime encodes a time.Time into a string.
func EncodeTime(ts time.Time) string {
	return ts.UTC().Format(HhmmssFormat)
}

func DecodeDateAndTime(datePart string, timePart string) (time.Time, error) {
	// Decode date
	dateInt, err := strconv.Atoi(datePart)
	if err != nil {
		return time.Time{}, fmt.Errorf("date part: %w", err)
	}
	yymmddInt := DateMax - dateInt

	year := (yymmddInt / 10000) + 2000
	month := (yymmddInt % 10000) / 100
	day := yymmddInt % 100

	if month < 1 || month > 12 {
		return time.Time{}, InvalidError("month out of range")
	}
	if day < 1 || day > 31 {
		return time.Time{}, InvalidError("day out of range")
	}

	// Decode time
	ts, err := time.Parse(HhmmssFormat, timePart)
	if err != nil {
		return time.Time{}, fmt.Errorf("time part: %w", err)
	}
	fullTime := time.Date(year, time.Month(month), day, ts.Hour(), ts.Minute(), ts.Second(), 0, time.UTC)
	return fullTime, nil
}
