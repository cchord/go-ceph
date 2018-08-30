package elector

func assert(c bool) {
	if !c {
		panic("condition not met")
	}
}
