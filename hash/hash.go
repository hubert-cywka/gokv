package hash

const (
	offset32 = 2166136261
	prime32  = 16777619
)

func Fnv32a(value string) uint32 {
	hash := uint32(offset32)
	for i := 0; i < len(value); i++ {
		hash ^= uint32(value[i])
		hash *= prime32
	}
	return hash
}
