package ovshelper

type Bridge struct {
	Name string 	`json:"name"`
	FailMode string `json:"fail_mode"`
}

type Port struct {
	Name string `json:"name"`
}

type Change struct {
	New Bridge `json:"new"`
	Old Bridge `json:"old"`
}

type Update struct {
	Bridge map[string]Change
}
