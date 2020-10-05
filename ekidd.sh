#!/usr/bin/env bash

exec docker run --rm -ti \
	-e http_proxy="${docker_http_proxy:-${http_proxy:-}}" \
	-e https_proxy="${docker_https_proxy:-${https_proxy:-}}" \
	-v $PWD:/home/rust/src \
	-v $PWD/ekidd-target:/home/rust/src/target \
	-v $PWD/ekidd-cache:/home/rust/.cargo/registry \
	ekidd/rust-musl-builder bash -c '
		sudo chown rust:rust target ~/.cargo/registry \
		&& cargo build --release --locked
	'
