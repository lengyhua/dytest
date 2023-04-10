package utils

func RemoveDeplicated(source []string) []string {
	r := make([]string, 0)
	var m = map[string]struct{}{}
	for _, k := range source {
		if _, ok := m[k]; !ok {
			m[k] = struct{}{}
			if k != "" {
				r = append(r, k)
			}
		}
	}
	return r
}

func Substract(source []string, sub []string) []string {
	subMap := make(map[string]struct{})
	for _, s := range sub {
		subMap[s] = struct{}{}
	}
	result := make([]string, 0)
	for _, s := range source {
		if _, ok := subMap[s]; !ok {
			result = append(result, s)
		}
	}
	return result
}

func IsIn(source []string, id string) bool {
	for _, i := range source {
		if i == id {
			return true
		}
	}
	return false
}
