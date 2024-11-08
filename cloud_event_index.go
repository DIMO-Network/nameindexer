package nameindexer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/ethereum/go-ethereum/common"
)

const (
	// FillerStatus is the filler value for status type cloud events.
	FillerStatus = "A"
	// FillerFingerprint is the filler value for fingerprint type cloud events.
	FillerFingerprint = "E"
	// FillerVerifiableCredential is the filler value for verifiable credential type cloud events.
	FillerVerifiableCredential = "V"
	// FillerUnknown is the filler value for unknown type cloud events.
	FillerUnknown = "U"
)

// CloudEventIndex represents the components of a decoded index.
type CloudEventIndex struct {
	// Subject is the subject of the data represented by the index.
	Subject cloudevent.NFTDID `json:"subject"`
	// Timestamp is the full timestamp used for date and time.
	Timestamp time.Time `json:"timestamp"`
	// PrimaryFiller is the filler value between the date and data type, typically "MM". If empty, defaults to "MM".
	PrimaryFiller string `json:"primaryFiller"`
	// DataType is the type of data, left-padded with ! or truncated to 30 characters.
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

// ToIndex converts a CloudEventIndex to an Index.
func (c CloudEventIndex) ToIndex() (Index, error) {
	subject := EncodeNFTDID(c.Subject)
	producer := EncodeNFTDID(c.Producer)
	unPrefixedSource := EncodeAddress(c.Source)

	return Index{
		Subject:         subject,
		Timestamp:       c.Timestamp,
		PrimaryFiller:   c.PrimaryFiller,
		Source:          unPrefixedSource,
		DataType:        c.DataType,
		Producer:        producer,
		SecondaryFiller: c.SecondaryFiller,
		Optional:        c.Optional,
	}, nil

}

// CloudTypeToFiller converts a cloud event type to a filler string.
func CloudTypeToFiller(status string) string {
	switch status {
	case cloudevent.TypeStatus:
		return FillerStatus
	case cloudevent.TypeFingerprint:
		return FillerFingerprint
	case cloudevent.TypeVerifableCredential:
		return FillerVerifiableCredential
	default:
		return FillerUnknown
	}
}

// FillerToCloudType converts a filler string to a cloud event type.
func FillerToCloudType(filler string) string {
	switch filler {
	case FillerStatus:
		return cloudevent.TypeStatus
	case FillerFingerprint:
		return cloudevent.TypeFingerprint
	case FillerVerifiableCredential:
		return cloudevent.TypeVerifableCredential
	default:
		return cloudevent.TypeUnknown
	}
}

// EncodeCloudEvent encodes a CloudEventHeader into a encoded indexable string.
func EncodeCloudEvent(cloudEvent *cloudevent.CloudEventHeader, secondaryFiller string) (string, error) {
	index, err := CloudEventToCloudIndex(cloudEvent, secondaryFiller)
	if err != nil {
		return "", fmt.Errorf("failed to convert cloud event to index: %w", err)
	}
	return EncodeCloudEventIndex(index)
}

// CloudEventToCloudIndex converts a CloudEventHeader to a CloudEventIndex.
func CloudEventToCloudIndex(cloudEvent *cloudevent.CloudEventHeader, secondaryFiller string) (*CloudEventIndex, error) {
	subjectDID, err := cloudevent.DecodeNFTDID(cloudEvent.Subject)
	if err != nil {
		return nil, fmt.Errorf("subject is not a valid NFT DID: %w", err)
	}
	producerDID, err := cloudevent.DecodeNFTDID(cloudEvent.Producer)
	if err != nil {
		return nil, fmt.Errorf("producer is not a valid NFT DID: %w", err)
	}
	sourceAddr, err := DecodeAddress(cloudEvent.Source)
	if err != nil {
		return nil, fmt.Errorf("source is not valid: %w", err)
	}
	if err := ValidateDate(cloudEvent.Time); err != nil {
		return nil, err
	}

	index := &CloudEventIndex{
		Subject:         subjectDID,
		Timestamp:       cloudEvent.Time,
		PrimaryFiller:   CloudTypeToFiller(cloudEvent.Type),
		Source:          sourceAddr,
		DataType:        cloudEvent.DataVersion,
		Producer:        producerDID,
		SecondaryFiller: secondaryFiller,
	}
	return index, nil
}

// CloudEventToPartialIndex converts a CloudEventHeader to a partial Index.
// This function is similar to CloudEventToCloudIndex, but it will not return an error if any parts are invalid.
func CloudEventToPartialIndex(cloudHdr *cloudevent.CloudEventHeader, secondaryFiller string) Index {
	if cloudHdr == nil {
		return Index{
			Timestamp: time.Now(),
		}
	}
	timestamp := cloudHdr.Time
	if err := ValidateDate(timestamp); err != nil {
		timestamp = time.Now()
	}
	subject := cloudHdr.Subject
	subjectDID, err := cloudevent.DecodeNFTDID(subject)
	if err == nil {
		subject = EncodeNFTDID(subjectDID)
	}
	producer := cloudHdr.Producer
	producerDID, err := cloudevent.DecodeNFTDID(producer)
	if err == nil {
		producer = EncodeNFTDID(producerDID)
	}
	source := cloudHdr.Source
	sourceAddr, err := DecodeAddress(source)
	if err == nil {
		source = EncodeAddress(sourceAddr)
	}
	return Index{
		Subject:         subject,
		Timestamp:       timestamp,
		PrimaryFiller:   CloudTypeToFiller(cloudHdr.Type),
		Source:          source,
		DataType:        cloudHdr.DataVersion,
		Producer:        producer,
		SecondaryFiller: secondaryFiller,
	}
}

// EncodeCloudEventIndex encodes a CloudEventIndex into a string.
func EncodeCloudEventIndex(cloudIndex *CloudEventIndex) (string, error) {
	if cloudIndex == nil {
		return "", InvalidError("index is nil")
	}
	index, err := cloudIndex.ToIndex()
	if err != nil {
		return "", fmt.Errorf("failed to convert cloud event index to index: %w", err)
	}
	return EncodeIndex(&index)
}

// DecodeCloudEvent decodes an encoded index string into a CloudEventHeader.
func DecodeCloudEvent(index string) (*cloudevent.CloudEventHeader, string, error) {
	decodedIndex, err := DecodeCloudEventIndex(index)
	if err != nil {
		return nil, "", fmt.Errorf("failed to decode index: %w", err)
	}
	cloudEvent := &cloudevent.CloudEventHeader{
		SpecVersion: "1.0",
		Subject:     decodedIndex.Subject.String(),
		Time:        decodedIndex.Timestamp,
		Type:        FillerToCloudType(decodedIndex.PrimaryFiller),
		DataVersion: decodedIndex.DataType,
		Producer:    decodedIndex.Producer.String(),
		Source:      decodedIndex.Source.Hex(),
	}
	return cloudEvent, decodedIndex.SecondaryFiller, nil
}

// DecodeCloudEventIndex decodes an index string into a cloudeventIndex
func DecodeCloudEventIndex(encodedIndex string) (*CloudEventIndex, error) {
	index, err := DecodeIndex(encodedIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode index: %w", err)
	}

	if !common.IsHexAddress(index.Source) {
		return nil, InvalidError("source is not a valid address")
	}
	source := common.HexToAddress(index.Source)
	subject, err := DecodeNFTDIDIndex(index.Subject)
	if err != nil {
		return nil, fmt.Errorf("subject part: %w", err)
	}
	producer, err := DecodeNFTDIDIndex(index.Producer)
	if err != nil {
		return nil, fmt.Errorf("producer part: %w", err)
	}

	decodedIndex := &CloudEventIndex{
		Subject:         subject,
		Timestamp:       index.Timestamp,
		PrimaryFiller:   index.PrimaryFiller,
		Source:          source,
		DataType:        index.DataType,
		Producer:        producer,
		SecondaryFiller: index.SecondaryFiller,
		Optional:        index.Optional,
	}

	return decodedIndex, nil
}

// EncodeAddress encodes an ethereum address without the 0x prefix.
func EncodeAddress(address common.Address) string {
	return address.Hex()[2:]
}

// DecodeAddress decodes an ethereum address from a string without the 0x prefix.
func DecodeAddress(encodedAddress string) (common.Address, error) {
	if !common.IsHexAddress(encodedAddress) {
		return common.Address{}, InvalidError("address is not a valid hex-encoded Ethereum address.")
	}
	return common.HexToAddress(encodedAddress), nil
}

// EncodeNFTDID encodes an NFTDID struct into an indexable string.
// This format is different from the standard NFTDID.
func EncodeNFTDID(did cloudevent.NFTDID) string {
	unPrefixedAddr := EncodeAddress(did.ContractAddress)
	return fmt.Sprintf("%016x%s%08x", did.ChainID, unPrefixedAddr, did.TokenID)
}

// DecodeNFTDIDIndex decodes an NFTDID string into a cloudevent.NFTDID struct.
func DecodeNFTDIDIndex(indexNFTDID string) (cloudevent.NFTDID, error) {
	if len(indexNFTDID) != DIDLength {
		return cloudevent.NFTDID{}, InvalidError("invalid NFTDID length")
	}
	var start int
	chainIDPart, start := getNextPart(indexNFTDID, start, 16)
	contractPart, start := getNextPart(indexNFTDID, start, AddressLength)
	tokenIDPart, _ := getNextPart(indexNFTDID, start, 8)

	chainID, err := strconv.ParseUint(chainIDPart, 16, 64)
	if err != nil {
		return cloudevent.NFTDID{}, fmt.Errorf("chain ID: %w", err)
	}

	contractAddress, err := DecodeAddress(contractPart)
	if err != nil {
		return cloudevent.NFTDID{}, fmt.Errorf("contract address: %w", err)
	}

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
