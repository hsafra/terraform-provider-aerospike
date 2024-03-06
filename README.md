# Terraform Provider For Aerospike Configuration and Objects

Terraform provider for Aerospike objects, like users, roles, ... 
This allows automating Aerospike objects as IaC, allowing reviewing changes and sharing config between environments easily 


## Requirements

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.0
- [Go](https://golang.org/doc/install) >= 1.21

## Using the provider

Initialize the provider with connection parmaeter in the provider block or environment variables.

## Future development
- XDR filters
- Secondary indexes

## Developing the Provider

If you wish to work on the provider, you'll first need [Go](http://www.golang.org) installed on your machine (see [Requirements](#requirements) above).

To compile the provider, run `go install`. This will build the provider and put the provider binary in the `$GOPATH/bin` directory.

To generate or update documentation, run `go generate`.

In order to run the full suite of Acceptance tests, run `make testacc`.

*Note:* Acceptance tests create real resources, and often cost money to run.

```shell
make testacc
```
