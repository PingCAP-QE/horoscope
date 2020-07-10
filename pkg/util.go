package pkg

import "math"

func QError(est, act float64) float64 {
	if act == 0 || est == 0 {
		return math.NaN()
	}
	z := est / act
	zp := act / est
	if z < 0 {
		return math.Inf(1)
	}
	return math.Max(z, zp)
}
