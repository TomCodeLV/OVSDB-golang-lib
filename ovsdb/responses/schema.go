package responses

type BaseType struct {
	Type string
	Enum []interface{}
	MinInteger int
	MaxInteger int
	MinReal float64
	MaxReal float64
	MinLength int
	MaxLength int
	RefTable string
	RefType string
}

type Type struct {
	Key BaseType
	Value BaseType
	Min int
	Max int
	MaxUnlimited string `json:"max"`
}

type Column struct {
	AtomicType string `json:"type"`
	Type Type `json:"type"`
	Ephemeral bool
	Mutable bool
}

type Table struct {
	Columns map[string]Column
	MaxRows int
	IsRoot bool
	Indexes []interface{}
}

type Schema struct {
	Name string `json:"name"`
	Version string `json:"version"`
	Cksum string `json:"cksum"`
	Tables map[string]Table `json:"tables"`
}