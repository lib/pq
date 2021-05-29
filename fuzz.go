package pq

func FuzzOpen(data []byte) int {
	_, err := Open(string(data))
	if err != nil {
		return 0
	}
	return 1
}
