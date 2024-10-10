package nameindexer

import (
	"fmt"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/ethereum/go-ethereum/common"
)

func cloudTypeToFiller(status string) string {
	switch status {
	case "status":
		return "0S"
	case "fingerprint":
		return "0F"
	case "connectivity":
		return "0C"
	default:
		return "00"
	}
}

func CloudEventToIndex(cloudEvent *cloudevent.CloudEventHeader, secondaryFiller string) (*Index, error) {
	subjectDID, err := cloudevent.DecodeNFTDID(cloudEvent.Subject)
	if err != nil {
		return nil, fmt.Errorf("subject is not a valid NFT DID: %w", err)
	}
	producerDID, err := cloudevent.DecodeNFTDID(cloudEvent.Producer)
	if err != nil {
		return nil, fmt.Errorf("producer is not a valid NFT DID: %w", err)
	}
	if !common.IsHexAddress(cloudEvent.Source) {
		return nil, InvalidError("source is not a valid address")
	}
	sourceAddr := common.HexToAddress(cloudEvent.Source)

	index := &Index{
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

func EncodeCloudEvent(cloudEvent *cloudevent.CloudEventHeader, secondaryFiller string) (string, error) {
	index, err := CloudEventToIndex(cloudEvent, secondaryFiller)
	if err != nil {
		return "", fmt.Errorf("failed to convert cloud event to index: %w", err)
	}
	return EncodeIndex(index)
}

type DID struct {
	ChainID         string         `json:"chainId"`
	ContractAddress common.Address `json:"contract"`
	TokenID         uint32         `json:"tokenId"`
}
