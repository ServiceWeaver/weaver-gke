# Echo

This directory contains a Service Weaver web application that echos the request
string back to the caller.

This application exists primarily to test the correctness of the GKE deployer.

## How to run?

To run this application locally in the GKE emulation mode,
run `weaver gke-local deploy`. To run the application on GKE,
run `weaver gke deploy`.

```console
$ weaver gke-local deploy weaver.toml    # Run locally.
$ weaver gke deploy weaver.toml          # Run on GKE.
```

## How to interact with the application?

```console
$ curl --header 'Host: echo.example.com' 'http://localhost:8000/?s=foo'
foo
```
