# Fixture Provenance

This fixture family is organized from the canonical artifacts for float `452.020-P-06`.

- `log/` contains canonical raw `LOG` files.
- `mer/` contains canonical raw `MER` files.
- `cycle/` contains canonical `CYCLE` files produced by automaid from concatenated `LOG` content.

This is an older-generation float that sent `LOG` directly rather than encrypted `BIN`, so there is no `bin/` fixture branch for this family.

Source revisions:

- automaid commit: `9a1b27013742f3ebc9f2268684f653a252c6628a`
- server commit: `2ff0dca66a386a66caa64415d666e4baf25401d6`
