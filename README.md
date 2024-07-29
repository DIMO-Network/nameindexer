# nameindexer

![GitHub license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
[![GoDoc](https://godoc.org/github.com/DIMO-Network/nameindexer?status.svg)](https://godoc.org/github.com/DIMO-Network/nameindexer)
[![Go Report Card](https://goreportcard.com/badge/github.com/DIMO-Network/nameindexer)](https://goreportcard.com/report/github.com/DIMO-Network/nameindexer)

This repository is responsible for creating and managing indexable names for objects.

The index string format is:

`date + primaryFiller + dataType + Subject + secondaryFiller + time`

where:

- `Date` is calculated as 999999 - (<two-digit-year>*10000 + <two-digit-month>*100 + <two-digit-day>)
  - Ex. To store The date Mar 8, 2024 it would be 999999 - 240308 = 759691 This will mean new dates have a lower number and thus get sorted first.
- `PrimaryFiller` is a constant string of length 2
- Two filler characters default "MM" (MM stands for MiddleMiddle)
- `DataType` is the data type left-padded with zeros or truncated to 10 characters
  - Ex. “Synth2.0.1” or “0Stat4.3.2"
- `Subject` is one of the following
  - Hexadecimal representation of the device's etherium address
  - TokenID prefixed with "T" left padded with zeros
  - IMEI prefixed with "IMEI" left padded with zeros
  - Ex. "0x12AE66CDc592e10B60f9097a7b0D3C59fce29876" or "T0000000000000001" or "IMEI000000000000001"
- `SecondaryFiller` is a constant string of length 2
  - 00 (Allows per vehicle sorting) Could be packet sequence number
- `Time` is the time in UTC in the format HHMMSS

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
