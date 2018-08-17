package helpers

// =======
// HELPERS
// =======

// Helper function for retrieving data from ovsdb set column.
func GetSet(data []interface{}) []interface{} {
	// if there is multiple entries data are returned as set
	if data[0] == "set" {
		return data[1].([]interface{})
	} else { // if there is one entry it is returned as single value
		return []interface{}{data}
	}
}

// Helper function to create ovsdb set
func MakeSet(data []interface{}) []interface{} {
	return []interface{}{
		"set",
		data,
	}
}

func RemoveFromUUIDList(UUIDList []interface{}, idsList []string) []interface{} {
	idMap := make(map[string]bool, len(UUIDList))
	for _, val := range idsList {
		idMap[val] = true
	}

	ret := make([]interface{}, 0)
	for _, uuid := range UUIDList {
		if _, ok := idMap[uuid.([]interface{})[1].(string)]; !ok {
			ret = append(ret, uuid)
		}
	}

	return ret
}

func AppendNamedUUIDToUUIDList(UUIDList []interface{}, idsList []string) []interface{} {
	ret := UUIDList
	for _, val := range idsList {
		ret = append(ret, []interface{}{"named-uuid", val})
	}
	return ret
}

//func MapToList(data interface{}) []interface{} {
//	switch data.(type) {
//	case map[string]interface{}:
//		d := data.(map[string]interface{})
//		l := make([]interface{}, len(d))
//		c := 0
//		for _, val := range d {
//			l[c] = val
//			c++
//		}
//		return l
//	default:
//		return nil
//	}
//}