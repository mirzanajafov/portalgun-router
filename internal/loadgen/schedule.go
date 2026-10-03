package loadgen

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
)

type Arrival string

const (
	Constant Arrival = "constant"
	Poisson  Arrival = "poisson"
)

type Schedule struct {
	interval float64
	poisson  bool
	rng      *rand.Rand
	i        int64
	t        float64
}

func NewSchedule(rate float64, arrival Arrival, seed uint64) (*Schedule, error) {
	if !(rate > 0) || math.IsInf(rate, 0) {
		return nil, errors.New("rate must be a positive number")
	}
	switch arrival {
	case Constant, Poisson:
	default:
		return nil, fmt.Errorf("unknown arrival %q", arrival)
	}
	return &Schedule{
		interval: 1e9 / rate,
		poisson:  arrival == Poisson,
		rng:      rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15)),
	}, nil
}

func (s *Schedule) Next() int64 {
	if s.poisson {
		at := s.t
		s.t += s.rng.ExpFloat64() * s.interval
		return int64(at)
	}
	at := float64(s.i) * s.interval
	s.i++
	return int64(at)
}
