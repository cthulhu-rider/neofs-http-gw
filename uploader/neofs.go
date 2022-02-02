package uploader

import (
	"context"
	"io"
	"time"
)

// Header describes HTTP header containing NeoFS-specific information.
type Header struct {
	// Length of the system prefix. Zero for non-system headers.
	SystemPrefixLen uint8

	// Non-empty header key.
	Key string

	// Non-empty header value.
	Value string
}

// HeadersMeta represents NeoFS-specific information about the group of HTTP headers.
type HeadersMeta struct {
	encountered struct {
		filename,
		timestamp,
		expiration bool
	}
}

// MetFilename indicates the presence of a header corresponding
// to the filename object attribute in NeoFS.
func (x *HeadersMeta) MetFilename() {
	x.encountered.filename = true
}

// MetTimestamp the presence of a header corresponding to the creation time
// object attribute in NeoFS.
func (x *HeadersMeta) MetTimestamp() {
	x.encountered.timestamp = true
}

// MetExpiration the presence of a header corresponding to the NeoFS epoch
// since object is expired.
func (x *HeadersMeta) MetExpiration() {
	x.encountered.expiration = true
}

// ObjectCreator represents component for creating objects in NeoFS.
// It unites stages of formation and recording of an object.
type ObjectCreator interface {
	// Close finishes writing of the prepared object to NeoFS.
	// Writes string ID of the new object.
	//
	// Writes true if saving failed.
	Close(*bool, *string)

	// UseBearerToken provides JSON-encoded bearer token which
	// should be used for the object creation.
	//
	// Writes true if token format is incorrect.
	UseBearerToken(*bool, []byte)

	// IntoContainer specifies the string ID of the container
	// into which the object should be stored.
	//
	// Writes true if ID format is incorrect.
	IntoContainer(*bool, string)

	// WriteHeader writes information from HTTP header to the new object.
	// Writes populated meta information to the dedicated parameter.
	WriteHeader(*HeadersMeta, Header)

	// FromFile specifies filename ???????????????
	FromFile(string)

	// CreatedAt specifies Unix time of the object creation.
	CreatedAt(time time.Time)

	// ExpireAfter marks object to be expired after the specified elapsed time.
	ExpireAfter(*bool, time.Duration)

	// ReadPayloadFrom specifies io.Reader primitive to read payload from.
	ReadPayloadFrom(io.Reader)
}

// NeoFS is an interface of the NeoFS network.
type NeoFS interface {
	// InitObjectCreation initiates creating of the NeoFS object with provided context.
	// Explicit compose is done by the ObjectCreator.
	//
	// IntoContainer and ReadPayloadFrom are always called.
	// Close is called most recent.
	InitObjectCreation(context.Context) ObjectCreator
}
