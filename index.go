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
	// DateLength is the length of the date part in the index string.
	DateLength = 6
	// PrimaryFillerLength is the length of the primary filler part in the index string.
	PrimaryFillerLength = 2
	// DataTypeLength is the length of the data type part in the index string.
	DataTypeLength = 10
	// AddressLength is the length of the address part in the index string.
	AddressLength = 40
	// SecondaryFillerLength is the length of the secondary filler part in the index string.
	SecondaryFillerLength = 2
	// TimeLength is the length of the time part in the index string.
	TimeLength = 6

	// DateStart is the starting position of the date part in the index string.
	DateStart = 0
	// PrimaryFillerStart is the starting position of the primary filler part in the index string.
	PrimaryFillerStart = DateStart + DateLength
	// DataTypeStart is the starting position of the data type part in the index string.
	DataTypeStart = PrimaryFillerStart + PrimaryFillerLength
	// AddressStart is the starting position of the address part in the index string.
	AddressStart = DataTypeStart + DataTypeLength
	// SecondaryFillerStart is the starting position of the secondary filler part in the index string.
	SecondaryFillerStart = AddressStart + AddressLength
	// TimeStart is the starting position of the time part in the index string.
	TimeStart = SecondaryFillerStart + SecondaryFillerLength
	// TotalLength is the total length of the index string.
	TotalLength = DateLength + PrimaryFillerLength + DataTypeLength + AddressLength + SecondaryFillerLength + TimeLength

	// DateMax is the maximum value used for date calculations in the index.
	DateMax = 999999
	// HhmmssFormat is the time format used in the index string.
	HhmmssFormat = "150405"

	// DefaultPrimaryFiller is the default filler value between the date and data type.
	DefaultPrimaryFiller = "MM"
	// DefaultSecondaryFiller is the default filler value between the address and time.
	DefaultSecondaryFiller = "00"
)

// InvalidError represents an error type for invalid arguments.
type InvalidError string

// Error implements the error interface for InvalidError.
func (e InvalidError) Error() string {
	return "invalid index " + string(e)
}

// Index represents the components of a decoded index.
type Index struct {
	// Timestamp is the full timestamp used for date and time.
	Timestamp time.Time
	// PrimaryFiller is the filler value between the date and data type, typically "MM". If empty, defaults to "MM".
	PrimaryFiller string
	// DataType is the type of data, left-padded with zeros or truncated to 10 characters.
	DataType string
	// Address is the Ethereum address of the device.
	Address common.Address
	// SecondaryFiller is the filler value between the address and time, typically "00". If empty, defaults to "00".
	SecondaryFiller string
}

// EncodeIndex creates an indexable name string from the Index struct.
// The index string format is:
//
//	date + primaryFiller + dataType + address + secondaryFiller + time
//
// where:
//   - date is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
//   - primaryFiller is a constant string of length 2
//   - dataType is the data type left-padded with zeros or truncated to 10 characters
//   - address is the hexadecimal representation of the device's address
//   - secondaryFiller is a constant string of length 2
//   - time is the time in UTC in the format HHMMSS
func EncodeIndex(index *Index) (string, error) {
	err := setDefaultsAndValidateIndex(index)
	if err != nil {
		return "", err
	}

	yymmddInt := (index.Timestamp.Year()%100)*10000 + int(index.Timestamp.Month())*100 + index.Timestamp.Day()
	datePart := DateMax - yymmddInt

	// Format time part
	timePart := index.Timestamp.UTC().Format(HhmmssFormat)

	// Construct the index string
	encodedIndex := fmt.Sprintf(
		"%06d%s%s%s%s%s",
		datePart,
		index.PrimaryFiller,
		index.DataType,
		index.Address.Hex()[2:], // Remove "0x" prefix
		index.SecondaryFiller,
		timePart,
	)

	return encodedIndex, nil
}

// DecodeIndex decodes an index string into its constituent parts.
// It returns an Index struct containing the decoded components.
// The index string format is expected to be:
//
//	date + primaryFiller + dataType + address + secondaryFiller + time
//
// where:
//   - date is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
//   - primaryFiller is a constant string of length 2
//   - dataType is the data type left-padded with zeros or truncated to 10 characters
//   - address is the hexadecimal representation of the device's address
//   - secondaryFiller is a constant string of length 2
//   - time is the time in UTC in the format HHMMSS
func DecodeIndex(index string) (*Index, error) {
	if len(index) != TotalLength {
		return nil, InvalidError(fmt.Sprintf("length %d is not %d", len(index), TotalLength))
	}

	// Extract parts of the index using start positions.
	datePart := index[DateStart:PrimaryFillerStart]              // 6 characters for date
	primaryFillerPart := index[PrimaryFillerStart:DataTypeStart] // 2 characters for primary filler
	dataTypePart := index[DataTypeStart:AddressStart]            // 10 characters for data type
	addressPart := index[AddressStart:SecondaryFillerStart]      // 40 characters for address
	secondaryFillerPart := index[SecondaryFillerStart:TimeStart] // 2 characters for secondary filler
	timePart := index[TimeStart:]                                // 6 characters for time

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

	// Decode address
	address := common.HexToAddress(addressPart)

	// Decode time
	ts, err := time.Parse(HhmmssFormat, timePart)
	if err != nil {
		return nil, fmt.Errorf("time part: %w", err)
	}
	fullTime := time.Date(year, time.Month(month), day, ts.Hour(), ts.Minute(), ts.Second(), 0, time.UTC)

	decodedIndex := &Index{
		Timestamp:       fullTime,
		PrimaryFiller:   primaryFillerPart,
		DataType:        strings.TrimLeft(dataTypePart, "0"),
		Address:         address,
		SecondaryFiller: secondaryFillerPart,
	}

	return decodedIndex, nil
}

func setDefaultsAndValidateIndex(index *Index) error {
	if index == nil {
		return InvalidError("nil index")
	}
	// Set default fillers if empty
	if index.PrimaryFiller == "" {
		index.PrimaryFiller = DefaultPrimaryFiller
	}
	if index.SecondaryFiller == "" {
		index.SecondaryFiller = DefaultSecondaryFiller
	}

	// Validate primary filler length
	if len(index.PrimaryFiller) != PrimaryFillerLength {
		return InvalidError("primary filler length")
	}
	// Validate secondary filler length
	if len(index.SecondaryFiller) != SecondaryFillerLength {
		return InvalidError("secondary filler length")
	}
	// Validate data type length
	if len(index.DataType) > DataTypeLength {
		return InvalidError("data type too long")
	}
	// Pad data type with zeros if shorter than required
	if len(index.DataType) < DataTypeLength {
		index.DataType = fmt.Sprintf("%0*s", DataTypeLength, index.DataType)
	}

	// Format date part
	if index.Timestamp.IsZero() || index.Timestamp.Year() < 2000 || index.Timestamp.Year() > 2099 {
		return InvalidError("timestamp year must be between 2000 and 2099")
	}

	return nil
}
