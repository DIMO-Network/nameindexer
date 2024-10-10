// Package legcay is legacy nameindexer provides utilities for creating, decoding, storing, and searching indexable names.
package legacy

import (
	"fmt"
	"regexp"
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
	// SubjectLenth is the length of the subject part in the index string.
	SubjectLenth = 40
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
	// SubjectStart is the starting position of the subject part in the index string.
	SubjectStart = DataTypeStart + DataTypeLength
	// SecondaryFillerStart is the starting position of the secondary filler part in the index string.
	SecondaryFillerStart = SubjectStart + SubjectLenth
	// TimeStart is the starting position of the time part in the index string.
	TimeStart = SecondaryFillerStart + SecondaryFillerLength
	// TotalLength is the total length of the index string.
	TotalLength = DateLength + PrimaryFillerLength + DataTypeLength + SubjectLenth + SecondaryFillerLength + TimeLength

	// DateMax is the maximum value used for date calculations in the index.
	DateMax = 999999
	// HhmmssFormat is the time format used in the index string.
	HhmmssFormat = "150405"

	// DefaultPrimaryFiller is the default filler value between the date and data type.
	DefaultPrimaryFiller = "MM"
	// DefaultSecondaryFiller is the default filler value between the subject and time.
	DefaultSecondaryFiller = "00"

	// TokenIDPrefix is the prefix for token IDs in the index string.
	TokenIDPrefix = "T"
	// IMEIPrefix is the prefix for IMEI strings in the index string.
	IMEIPrefix = "IMEI"
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
	// Timestamp is the full timestamp used for date and time.
	Timestamp time.Time
	// PrimaryFiller is the filler value between the date and data type, typically "MM". If empty, defaults to "MM".
	PrimaryFiller string
	// DataType is the type of data, left-padded with zeros or truncated to 10 characters.
	DataType string
	// Subject is the subject of the data represented by the index.
	Subject Subject
	// SecondaryFiller is the filler value between the subject and time, typically "00". If empty, defaults to "00".
	SecondaryFiller string
}

// Subject represents the subject of the data represented by the index.
// The subject can be an Ethereum address or a token ID.
// if both are set, the address is used.
type Subject struct {
	Identifier IsIdentifier
}

// IsIdentifier is an interface for subject types.
type IsIdentifier interface {
	isIdentifier()
}

// Address is a subject type representing an Ethereum address.
type Address common.Address

func (Address) isIdentifier() {}

// TokenID is a subject type representing a token ID.
type TokenID uint32

func (TokenID) isIdentifier() {}

// IMEI is a subject type representing an IMEI string.
type IMEI string

func (IMEI) isIdentifier() {}

// String encodes a subject into a string and satisfies the fmt.Stringer interface.
func (s Subject) String() string {
	switch sub := s.Identifier.(type) {
	case Address:
		return common.Address(sub).Hex()[2:] // Remove "0x" prefix
	case TokenID:
		return fmt.Sprintf("%s%0*d", TokenIDPrefix, SubjectLenth-len(TokenIDPrefix), sub)
	case IMEI:
		return fmt.Sprintf("%s%0*s", IMEIPrefix, SubjectLenth-(len(IMEIPrefix)), sub)
	default:
		return ""
	}
}

// Value satisfies sql/driver.Valuer interface for Subject.
func (s Subject) Value() (string, error) {
	return s.String(), nil
}

// Scan satisfies sql.Scanner interface for Subject.
func (s *Subject) Scan(value any) error {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case string:
		subject, err := DecodeSubject(v)
		if err != nil {
			return err
		}
		*s = subject
		return nil
	default:
		return InvalidError("invalid subject type")
	}
}

// DecodeSubject decodes a string into a subject.
func DecodeSubject(encoded string) (Subject, error) {
	if strings.HasPrefix(encoded, TokenIDPrefix) && len(encoded) > len(TokenIDPrefix) {
		tokenID64, err := strconv.ParseUint(encoded[len(TokenIDPrefix):], 10, 32)
		if err != nil {
			return Subject{}, fmt.Errorf("token ID: %w", err)
		}
		tokenID := uint32(tokenID64)
		return Subject{TokenID(tokenID)}, nil
	}
	if strings.HasPrefix(encoded, IMEIPrefix) && len(encoded) > len(IMEIPrefix) {
		imei := IMEI(encoded[len(TokenIDPrefix):])
		return Subject{imei}, nil
	}

	address := common.HexToAddress(encoded)
	return Subject{Address(address)}, nil
}

// EncodeIndex creates an indexable name string from the Index struct.
// This function will modify the index to have correctly padded values.
// The index string format is:
//
//	date + primaryFiller + dataType + Subject + secondaryFiller + time
//
// where:
//   - date is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
//   - primaryFiller is a constant string of length 2
//   - dataType is the data type left-padded with zeros or truncated to 10 characters
//   - subject is one of the following
//     1. hexadecimal representation of the device's address
//     2. TokenID prefixed with "T"
//     3. IMEI prefixed with "IMEI"
//   - secondaryFiller is a constant string of length 2
//   - time is the time in UTC in the format HHMMSS
func EncodeIndex(index *Index) (string, error) {
	err := SetDefaultsAndValidateIndex(index)
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
		index.Subject,
		index.SecondaryFiller,
		timePart,
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

	// Extract parts of the index using start positions.
	datePart := index[DateStart:PrimaryFillerStart]              // 6 characters for date
	primaryFillerPart := index[PrimaryFillerStart:DataTypeStart] // 2 characters for primary filler
	dataTypePart := index[DataTypeStart:SubjectStart]            // 10 characters for data type
	subjectPart := index[SubjectStart:SecondaryFillerStart]      // 40 characters for subject
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

	subject, err := DecodeSubject(subjectPart)
	if err != nil {
		return nil, fmt.Errorf("subject part: %w", err)
	}

	// Decode time
	ts, err := time.Parse(HhmmssFormat, timePart)
	if err != nil {
		return nil, fmt.Errorf("time part: %w", err)
	}
	fullTime := time.Date(year, time.Month(month), day, ts.Hour(), ts.Minute(), ts.Second(), 0, time.UTC)

	decodedIndex := &Index{
		Timestamp:       fullTime,
		PrimaryFiller:   primaryFillerPart,
		DataType:        dataTypePart,
		Subject:         subject,
		SecondaryFiller: secondaryFillerPart,
	}

	return decodedIndex, nil
}

// SetDefaultsAndValidateIndex sets default values for empty fields and validates the index.
// This function will modify the index to have correctly padded values.
func SetDefaultsAndValidateIndex(index *Index) error {
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
	var err error
	index.DataType, err = SantatizeDataType(index.DataType)
	if err != nil {
		return err
	}
	// check that imei is valid regex
	if imei, ok := index.Subject.Identifier.(IMEI); ok {
		if !digitRegex.MatchString(string(imei)) {
			return InvalidError(fmt.Sprintf("IMEI %s can only be digits", imei))
		}
		if len(imei) == 14 {
			index.Subject.Identifier = IMEI(string(imei) + calculateCheckDigit(string(imei)))
		} else if len(imei) != 15 {
			return InvalidError("IMEI must be 14 or 15 digits")
		}
	}

	// Format date part
	if index.Timestamp.IsZero() || index.Timestamp.Year() < 2000 || index.Timestamp.Year() > 2099 {
		return InvalidError("timestamp year must be between 2000 and 2099")
	}

	return nil
}

// SantatizeDataType pads the data type with zeros if shorter than required.
// returns an error if the data type is too long.
func SantatizeDataType(dataType string) (string, error) {
	// Validate data type length
	if len(dataType) > DataTypeLength {
		return "", InvalidError("data type too long")
	}
	// Pad data type with zeros if shorter than required
	if len(dataType) < DataTypeLength {
		return fmt.Sprintf("%0*s", DataTypeLength, dataType), nil
	}
	return dataType, nil
}

// calculateCheckDigit calculates the check digit for an IMEI string. using the Luhn algorithm.
func calculateCheckDigit(imei string) string {
	// convert to a slice of digits
	digits := make([]int, len(imei))
	for i, r := range imei {
		digits[i] = int(r - '0')
	}
	// calculate the check digit
	sum := 0
	for i := 0; i < len(digits); i++ {
		if i%2 == 1 {
			digits[i] *= 2
			if digits[i] > 9 {
				digits[i] -= 9
			}
		}
		sum += digits[i]
	}
	remainder := sum % 10
	if remainder == 0 {
		return "0"
	}
	checkDigit := (10 - remainder)
	return strconv.Itoa(checkDigit)
}
