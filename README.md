#  FLOW

Simple read-process-right worker with goroutines.

## Usage

1. Register ```In```, ```Out``` and ```Process``` functions, that implement corresponding interfaces.
2. Run ```flow.Serve``` 