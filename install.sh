#!/bin/sh
set -eu

printf >&2 '%s\n' \
  'Error: the upstream installer is disabled for this fork.' \
  'Build a selected fork revision with `cargo build --release --locked`.'
exit 1
