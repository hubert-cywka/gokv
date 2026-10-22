package statefile

type Codec[T any] interface {
	Size() int
	Zero() T

	Marshal(T, []byte)
	Unmarshal([]byte) T
}
