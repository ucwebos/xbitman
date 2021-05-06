package conf

const (
	TypeBool   = 1
	TypeInt    = 2
	TypeFloat  = 3
	TypeString = 4

	DBNAME = "X"
)

var (
	G = Conf{Path: "ZDATA"}
)

var MyInfos = &Table{
	Name: "myinfos",
	PKey: &SchemeKey{
		Key:  "id",
		Type: TypeInt,
	},
	Indexes: []*SchemeKey{
		{
			Key:  "uid",
			Type: TypeInt,
		},
		{
			Key:  "name",
			Type: TypeString,
		},
		{
			Key:  "age",
			Type: TypeInt,
		},
	},
}

type Conf struct {
	Path string `json:"path"`
}

type SchemeKey struct {
	Key  string `json:"key"`
	Type int    `json:"type"`
}

type Table struct {
	Name    string       `json:"name"`
	PKey    *SchemeKey   `json:"pKey"`
	Indexes []*SchemeKey `json:"indexes"`
}
