package conf

const (
	TypeBool   = 1
	TypeInt    = 2
	TypeFloat  = 3
	TypeString = 4
	TypeSet    = 5 //集合
	TypeMulti  = 6 //子表 1对多

	DBNAME = "X"
)

var (
	G = Conf{Path: "ZDATA"}
)

type Conf struct {
	Path string `json:"path"`
}

type SchemeKey struct {
	Key        string       `json:"key"`
	Type       int          `json:"type"`
	SubIndexes []*SchemeKey `json:"subIndexes,omitempty"`
}

type Table struct {
	Name    string       `json:"name"`
	PKey    *SchemeKey   `json:"pKey"`
	Indexes []*SchemeKey `json:"indexes"`
}
