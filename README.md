# nameindexer

![GitHub license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
[![GoDoc](https://godoc.org/github.com/DIMO-Network/nameindexer?status.svg)](https://godoc.org/github.com/DIMO-Network/nameindexer)
[![Go Report Card](https://goreportcard.com/badge/github.com/DIMO-Network/nameindexer)](https://goreportcard.com/report/github.com/DIMO-Network/nameindexer)

This repository is responsible for creating and managing indexable names for objects.

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
