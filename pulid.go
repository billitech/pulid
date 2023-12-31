package pulid

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
)

// PULID implements a PULID - a prefixed ULID.
type PULID [18]byte

const EncodedSize = 28

var (
	ErrPrefixLength = errors.New("pulid: bad prefix length")

	Nil = PULID{}
)

// New returns a PULID with the given Unix milliseconds timestamp and an
// optional entropy source. Use the Timestamp function to convert
// a time.Time to Unix milliseconds.
//
// ErrPrefixLength is returned when passing a prefix bigger or smaller than 2.
//
// ErrBigTime is returned when passing a timestamp bigger than MaxTime.
// Reading from the entropy source may also return an error.
//
// Safety for concurrent use is only dependent on the safety of the
// entropy source.
func New(prefix string, ms uint64, entropy io.Reader) (PULID, error) {
	id := PULID{}
	if len(prefix) != 2 {
		return PULID{}, ErrPrefixLength
	}
	ulid, err := ulid.New(ms, entropy)
	if err != nil {
		return PULID{}, err
	}

	join([]byte(prefix), ulid, &id)

	return id, nil
}

// MustNew is a convenience function equivalent to New that panics on failure
// instead of returning an error.
func MustNew(prefix string, ms uint64, entropy io.Reader) PULID {
	id, err := New(prefix, ms, entropy)
	if err != nil {
		panic(err)
	}
	return id
}

// MustNewDefault is a convenience function equivalent to MustNew with
// DefaultEntropy as the entropy. It may panic if the given time.Time is too
// large or too small.
// Also panic if prefix is not of length 2.
func MustNewDefault(prefix string, t time.Time) PULID {
	return MustNew(prefix, ulid.Timestamp(t), ulid.DefaultEntropy())
}

// Make returns a PULID with the current time in Unix milliseconds and
// monotonically increasing entropy for the same millisecond.
// It is safe for concurrent use, leveraging a sync.Pool underneath for minimal
// contention.
// Panic if prefix is not of length 2.
func Make(prefix string) PULID {
	return MustNew(prefix, ulid.Now(), ulid.DefaultEntropy())
}

// Parse parses an encoded PULID, returning an error in case of failure.
//
// ErrDataSize is returned if the len(ulid) is different from an encoded
// ULID's length. Invalid encodings produce undefined ULIDs. For a version that
// returns an error instead, see ParseStrict.
func Parse(id string) (PULID, error) {
	pulid := PULID{}
	return pulid, parseBytes([]byte(id), &pulid)
}

// ParseStrict parses an encoded PULID, returning an error in case of failure.
//
// It is like Parse, but additionally validates that the parsed ULID consists
// only of valid base32 characters. It is slightly slower than Parse.
//
// ErrDataSize is returned if the len(ulid) is different from an encoded
// ULID's length. Invalid encodings return ErrInvalidCharacters.
func ParseStrict(id string) (PULID, error) {
	pulid := PULID{}
	if len(id) != EncodedSize {
		return pulid, ulid.ErrDataSize
	}

	ulid, err := ulid.ParseStrict(id[2:])

	if err != nil {
		return pulid, err
	}

	join([]byte(id[:2]), ulid, &pulid)

	return pulid, nil
}

func parseBytes(v []byte, id *PULID) error {
	if len(v) != EncodedSize {
		return ulid.ErrDataSize
	}

	ulid := ulid.ULID{}
	err := ulid.UnmarshalText(v[2:])

	if err != nil {
		return err
	}

	join(v[:2], ulid, id)

	return nil
}

// MustParse is a convenience function equivalent to Parse that panics on failure
// instead of returning an error.
func MustParse(id string) PULID {
	pulid, err := Parse(id)
	if err != nil {
		panic(err)
	}
	return pulid
}

// MustParseStrict is a convenience function equivalent to ParseStrict that
// panics on failure instead of returning an error.
func MustParseStrict(id string) PULID {
	pulid, err := ParseStrict(id)
	if err != nil {
		panic(err)
	}
	return pulid
}

// Bytes returns bytes slice representation of PULID.
func (id PULID) Bytes() []byte {
	return id[:]
}

// ULID returns ULID from the PULID
func (id PULID) ULID() ulid.ULID {
	ulid := ulid.ULID{}
	ulid.UnmarshalBinary(id[2:])
	return ulid
}

// ULID returns ULID from the PULID
func (id PULID) PrefixBytes() []byte {
	return id[:2]
}

// ULID returns ULID from the PULID
func (id PULID) Prefix() string {
	return string(id.PrefixBytes())
}

// String returns a lexicographically sortable string encoded PULID
// (26 characters, non-standard base 32) e.g. PR01AN4Z07BY79KA1307SR9X4MV3.
// Format: pptttttttttteeeeeeeeeeeeeeee where p is prefix t is time and e is entropy.
func (id PULID) String() string {
	pulid := make([]byte, EncodedSize)
	_ = id.MarshalTextTo(pulid)
	return string(pulid)
}

// MarshalBinary implements the encoding.BinaryMarshaler interface by
// returning the ULID as a byte slice.
func (id PULID) MarshalBinary() ([]byte, error) {
	ulid := make([]byte, len(id))
	return ulid, id.MarshalBinaryTo(ulid)
}

// MarshalBinaryTo writes the binary encoding of the ULID to the given buffer.
// ErrBufferSize is returned when the len(dst) != 16.
func (id PULID) MarshalBinaryTo(dst []byte) error {
	if len(dst) != len(id) {
		return ulid.ErrBufferSize
	}

	copy(dst, id[:])
	return nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface by
// copying the passed data and converting it to a ULID. ErrDataSize is
// returned if the data length is different from ULID length.
func (id *PULID) UnmarshalBinary(data []byte) error {
	if len(data) != len(*id) {
		return ulid.ErrDataSize
	}

	copy((*id)[:], data)
	return nil
}

// MarshalText implements the encoding.TextMarshaler interface by
// returning the string encoded ULID.
func (id PULID) MarshalText() ([]byte, error) {
	ulid := make([]byte, EncodedSize)
	return ulid, id.MarshalTextTo(ulid)
}

// MarshalTextTo writes the ULID as a string to the given buffer.
// ErrBufferSize is returned when the len(dst) != 26.
func (id PULID) MarshalTextTo(dst []byte) error {
	if id == Nil {
		for i := range dst {
			dst[i] = 48
		}
		return nil
	}

	ulidBytes, err := id.ULID().MarshalText()
	if err != nil {
		return err
	}

	prefixBytes := id.PrefixBytes()
	copy(dst, prefixBytes)

	for i := range ulidBytes {
		dst[i+len(prefixBytes)] = ulidBytes[i]
	}

	return nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface by
// parsing the data as string encoded ULID.
//
// ErrDataSize is returned if the len(v) is different from an encoded
// ULID's length. Invalid encodings produce undefined ULIDs.
func (id *PULID) UnmarshalText(v []byte) error {
	return parseBytes(v, id)
}

// Time returns the Unix time in milliseconds encoded in the ULID.
// Use the top level Time function to convert the returned value to
// a time.Time.
func (id PULID) Time() uint64 {
	return id.ULID().Time()
}

// Timestamp returns the time encoded in the ULID as a time.Time.
func (id PULID) Timestamp() time.Time {
	return ulid.Time(id.Time())
}

// SetTime sets the time component of the ULID to the given Unix time
// in milliseconds.
func (id *PULID) SetTime(ms uint64) error {
	ulid := id.ULID()
	err := ulid.SetTime(ms)
	if err != nil {
		return err
	}

	join(id.PrefixBytes(), ulid, id)
	return nil
}

// Entropy returns the entropy from the ULID.
func (id PULID) Entropy() []byte {
	return id.ULID().Entropy()
}

// SetEntropy sets the ULID entropy to the passed byte slice.
// ErrDataSize is returned if len(e) != 10.
func (id *PULID) SetEntropy(e []byte) error {
	ulid := id.ULID()
	err := ulid.SetEntropy(e)
	if err != nil {
		return err
	}

	join(id.PrefixBytes(), ulid, id)
	return nil
}

// Compare returns an integer comparing id and other lexicographically.
// The result will be 0 if id==other, -1 if id < other, and +1 if id > other.
func (id PULID) Compare(other PULID) int {
	return bytes.Compare(id[:], other[:])
}

// Scan implements the sql.Scanner interface. It supports scanning
// a string or byte slice.
func (id *PULID) Scan(src interface{}) error {
	switch x := src.(type) {
	case nil:
		return nil
	case PULID:
		*id = src.(PULID)
		return nil
	case string:
		return id.UnmarshalText([]byte(x))
	case []byte:
		return id.UnmarshalBinary(x)
	}

	return ulid.ErrScanValue
}

// Value implements the sql/driver.Valuer interface, returning the ULID as a
func (id PULID) Value() (driver.Value, error) {
	return id.String(), nil
}

// UnmarshalGQL implements the graphql.Unmarshaler interface
func (id *PULID) UnmarshalGQL(v interface{}) error {
	return id.Scan(v)
}

// MarshalGQL implements the graphql.Marshaler interface
func (id PULID) MarshalGQL(w io.Writer) {
	_, _ = io.WriteString(w, strconv.Quote(id.String()))
}

func join(prefix []byte, ulid ulid.ULID, id *PULID) {
	id[0] = prefix[0]
	id[1] = prefix[1]
	for i := range ulid {
		id[i+2] = ulid[i]
	}
}
