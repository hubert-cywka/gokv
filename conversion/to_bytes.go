package conversion

import "encoding/binary"

func Uint64ToBytes(val uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, val)
	return buf
}

func Uint32ToBytes(val uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, val)
	return buf
}

func Uint16ToBytes(val uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, val)
	return buf
}

func Uint8ToBytes(val uint8) []byte {
	return []byte{val}
}
