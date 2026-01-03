package internal

func consumeWhitespace(input string, i int, n int) int {
	for i < n && (input[i] == ' ' || input[i] == '\t' || input[i] == '\n' || input[i] == '\r') {
		i++
	}
	return i
}

func consumeLineComment(input string, i int, n int) int {
	i += 2 // skip '--'
	for i < n && input[i] != '\n' {
		i++
	}
	return i
}

func consumeBlockComment(input string, i int, n int) int {
	i += 2 // skip '/*'
	for i < n-1 {
		if input[i] == '*' && input[i+1] == '/' {
			i += 2
			return i
		}
		i++
	}
	// Unterminated comment? Consider as done consuming.
	return i
}

func StartsWithCOPY(input string) bool {
	const (
		Start = iota
		WhitespaceOrComment
		C
		O
		P
		Y
		Done
		Fail
	)

	state := Start
	i := 0
	n := len(input)

	for state != Done && state != Fail {
		if i >= n {
			if state == Y {
				state = Done
			} else {
				state = Fail
			}
			break
		}

		switch state {
		case Start, WhitespaceOrComment:
			i = consumeWhitespace(input, i, n)
			if i+1 < n && input[i] == '-' && input[i+1] == '-' {
				i = consumeLineComment(input, i, n)
			} else if i+1 < n && input[i] == '/' && input[i+1] == '*' {
				i = consumeBlockComment(input, i, n)
			} else if i < n {
				switch input[i] {
				case 'C', 'c':
					state = C
					i++
				case ' ', '\t', '\n', '\r':
					// handled in consumeWhitespace
				default:
					state = Fail
				}
			}
		case C:
			if i < n && (input[i] == 'O' || input[i] == 'o') {
				state = O
				i++
			} else {
				state = Fail
			}
		case O:
			if i < n && (input[i] == 'P' || input[i] == 'p') {
				state = P
				i++
			} else {
				state = Fail
			}
		case P:
			if i < n && (input[i] == 'Y' || input[i] == 'y') {
				state = Y
				i++
			} else {
				state = Fail
			}
		case Y:
			state = Done
		}
	}

	return state == Done
}
