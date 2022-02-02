package downloader

import (
	"github.com/nspcc-dev/neofs-sdk-go/object/address"

	"github.com/nspcc-dev/neofs-sdk-go/pool"
)

const sizeToDetectType = 512

func (r request) headObject(clnt pool.Object, objectAddress *address.Address) {
	// 1. read CID
	// 2. read OID or attribute (selector)
	// 3. read bearer
	// 4. call and handle NeoFS API error

	// Content-Length (payload size)
	// Attributes like in GET but w/o filename
	// x-object-id, x-owner-id, x-container-id
	// content type
}
