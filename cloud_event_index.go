package nameindexer

import (
	"fmt"
	"strconv"
	"strings"
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

// CloudTypeToFiller converts a cloud event type to a filler string.
func CloudTypeToFiller(eventTypes string) string {
	firstStatus := strings.Split(eventTypes, ",")[0]
	switch firstStatus {
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
	index, err := CloudEventToIndex(cloudEvent, secondaryFiller)
	if err != nil {
		return "", fmt.Errorf("failed to convert cloud event to index: %w", err)
	}
	return EncodeIndex(&index)
}

// CloudEventToIndex converts a CloudEventHeader to a CloudEventIndex.
func CloudEventToIndex(cloudEvent *cloudevent.CloudEventHeader, secondaryFiller string) (Index, error) {
	subjectDID, err := cloudevent.DecodeNFTDID(cloudEvent.Subject)
	if err != nil {
		return Index{}, fmt.Errorf("subject is not a valid NFT DID: %w", err)
	}
	producerDID, err := cloudevent.DecodeNFTDID(cloudEvent.Producer)
	if err != nil {
		return Index{}, fmt.Errorf("producer is not a valid NFT DID: %w", err)
	}
	sourceAddr, err := DecodeAddress(cloudEvent.Source)
	if err != nil {
		return Index{}, fmt.Errorf("source is not valid: %w", err)
	}
	if err := ValidateDate(cloudEvent.Time); err != nil {
		return Index{}, err
	}

	index := Index{
		Subject:         EncodeNFTDID(subjectDID),
		Timestamp:       cloudEvent.Time,
		PrimaryFiller:   CloudTypeToFiller(cloudEvent.Type),
		Source:          EncodeAddress(sourceAddr),
		DataType:        cloudEvent.DataVersion,
		Producer:        EncodeNFTDID(producerDID),
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
