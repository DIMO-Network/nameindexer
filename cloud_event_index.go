package nameindexer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/ethereum/go-ethereum/common"
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
	subject, err := EncodeNFTDID(c.Subject)
	if err != nil {
		return Index{}, fmt.Errorf("subject: %w", err)
	}
	producer, err := EncodeNFTDID(c.Producer)
	if err != nil {
		return Index{}, fmt.Errorf("producer: %w", err)
	}

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

func cloudTypeToFiller(status string) string {
	switch status {
	case "dimo.status":
		return "MA"
	case "dimo.fingerprint":
		return "ME"
	default:
		return "MU"
	}
}

// FillerToCloudType converts a filler string to a cloud event type.
func FillerToCloudType(filler string) string {
	switch filler {
	case "MA":
		return "dimo.status"
	case "ME":
		return "dimo.fingerprint"
	default:
		return "dimo.unknown"
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

	index := &CloudEventIndex{
		Subject:         subjectDID,
		Timestamp:       cloudEvent.Time,
		PrimaryFiller:   cloudTypeToFiller(cloudEvent.Type),
		Source:          sourceAddr,
		DataType:        cloudEvent.DataVersion,
		Producer:        producerDID,
		SecondaryFiller: secondaryFiller,
	}
	return index, nil
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
func EncodeNFTDID(did cloudevent.NFTDID) (string, error) {
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
