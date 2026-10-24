package record

type Appender interface {
	Append(record *Record) error
}
