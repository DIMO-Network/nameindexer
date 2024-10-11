# nameindexer

![GitHub license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
[![GoDoc](https://godoc.org/github.com/DIMO-Network/nameindexer?status.svg)](https://godoc.org/github.com/DIMO-Network/nameindexer)
[![Go Report Card](https://goreportcard.com/badge/github.com/DIMO-Network/nameindexer)](https://goreportcard.com/report/github.com/DIMO-Network/nameindexer)

This repository is responsible for creating and managing indexable names for objects.

The index string format is:

subject + date + time + primaryFiller + source + dataType + secondaryFiller + producer + optional

where:

- subject is the NFTDID of the data's subject
  - chainId + contractAddress + tokenID
  - chainId is a 16-character hexadecimal string representing the uint64 chain ID
  - contractAddress is a 40-character hexadecimal string representing the contract address
  - tokenID is an 8-character hexadecimal string representing the uint32 token ID
- date is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
- time is the time in UTC in the format HHMMSS
- primaryFiller is a constant string of length 2
- source is a 40-character hexadecimal string representing the source address
- dataType is the data type left-padded with `!` or truncated to 20 characters
- secondaryFiller is a constant string of length 2
- producer is the NFTDID of the data's producer
  - chainId + contractAddress + tokenID
  - chainId is a 16-character hexadecimal string representing the uint64 chain ID
  - contractAddress is a 40-character hexadecimal string representing the contract address
  - tokenID is an 8-character hexadecimal string representing the uint32 token ID
- optional is an optional string that can be appended to the index

# Development

Use `make` to manage the project building, testing, and linting.

```
> make help

Specify a subcommand:

  build                Build the code
  clean                Clean the project binaries
  tidy                 tidy the go modules
  test                 Run the all tests
  lint                 Run the linter
  format               Run the linter with fix
  migration            Generate migration file specify name with name=your_migration_name
  tools                Install all tools
  tools-golangci-lint  Install golangci-lint
  tools-migration      Install migration tool
```

## License

[Apache 2.0](LICENSE)
