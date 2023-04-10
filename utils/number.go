package utils

import "strconv"

func StringToInt(v string, defaultV int) int {
	i, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return defaultV
	}
	return int(i)
}

func StringToFloat(v string, defaultV float32) float32 {
	f, err := strconv.ParseFloat(v, 32)
	if err != nil {
		return 0.0
	}
	return float32(f)
}
