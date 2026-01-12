package tsm1

/*
This code is originally from: https://github.com/dgryski/go-tsz and has been modified to remove
the timestamp compression fuctionality.

It implements the float compression as presented in: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
This implementation uses a sentinel value of NaN which means that float64 NaN cannot be stored using
this version.
*/

// Note: an uncompressed format is not yet implemented.
// floatCompressedGorilla is a compressed format using the gorilla paper encoding
const floatCompressedGorilla = 1

// uvnan is the constant returned from math.NaN().
const uvnan = 0x7FF8000000000001
