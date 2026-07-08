switch("path", "$projectDir/../src")
# TLS is part of v1 (Phase 8); build test binaries with SSL support so
# https construction/connection paths compile and run.
switch("define", "ssl")