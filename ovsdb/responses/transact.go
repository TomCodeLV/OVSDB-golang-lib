package responses

type Row struct {
	Name string
	UUID UUID 		`json:"_uuid"`
}

type ActionResponse struct {
	Rows []Row
	UUID UUID
	Error string
	Details string
}

type Transact []ActionResponse
