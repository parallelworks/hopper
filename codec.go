package hopper

import "encoding/json"

// Codec encodes job args and outputs for storage. The encoded form must be
// valid JSON, because it is stored in a jsonb column; a codec that encrypts
// or compresses wraps its output in a JSON string. The default is
// encoding/json.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec is the default Codec, using encoding/json.
type JSONCodec struct{}

// Marshal implements Codec.
func (JSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements Codec.
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
