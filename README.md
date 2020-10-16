#  FLOW

> Simple read-process-write worker with goroutines

[![GoDoc](https://godoc.org/github.com/mehanizm/flow?status.svg)](https://pkg.go.dev/github.com/mehanizm/flow)
![Go](https://github.com/mehanizm/flow/workflows/Go/badge.svg)
[![codecov](https://codecov.io/gh/mehanizm/flow/branch/master/graph/badge.svg)](https://codecov.io/gh/mehanizm/flow)
[![Go Report](https://goreportcard.com/badge/github.com/mehanizm/flow)](https://goreportcard.com/report/github.com/mehanizm/flow)

## Usage

1. Register ```In```, ```Out``` and ```Processes``` functions, that implement corresponding interfaces.
2. Run ```flow.Serve``` 
