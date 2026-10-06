package loadgen

import (
	"errors"
	"math/rand/v2"
	"net/url"
	"strconv"
	"time"
)

type picker struct {
	urls  []*url.URL
	cum   []float64
	rev   []float64
	shift int64
	rng   *rand.Rand
	keys  []string
	krng  *rand.Rand
	zipf  *rand.Zipf
}

func cumulative(ws []float64) []float64 {
	out := make([]float64, len(ws))
	var sum float64
	for i, w := range ws {
		sum += w
		out[i] = sum
	}
	for i := range out {
		out[i] /= sum
	}
	return out
}

func newPicker(cfg Config, urls []*url.URL) (*picker, error) {
	p := &picker{urls: urls, shift: int64(cfg.ShiftEvery)}
	if len(cfg.Weights) > 0 {
		if len(cfg.Weights) != len(urls) {
			return nil, errors.New("weights need one value per url")
		}
		var sum float64
		for _, w := range cfg.Weights {
			if !(w >= 0) {
				return nil, errors.New("weights must not be negative")
			}
			sum += w
		}
		if !(sum > 0) {
			return nil, errors.New("weights must not all be zero")
		}
		reversed := make([]float64, len(cfg.Weights))
		for i, w := range cfg.Weights {
			reversed[len(reversed)-1-i] = w
		}
		p.cum, p.rev = cumulative(cfg.Weights), cumulative(reversed)
		p.rng = rand.New(rand.NewPCG(cfg.Seed, 0x5851f42d4c957f2d))
	} else if cfg.ShiftEvery > 0 {
		return nil, errors.New("shifting needs weights")
	}
	if cfg.ShiftEvery < 0 {
		return nil, errors.New("shift period must not be negative")
	}
	if cfg.KeyHeader == "" {
		return p, nil
	}
	n := max(cfg.Keys, 1)
	p.keys = make([]string, n)
	for i := range p.keys {
		p.keys[i] = cfg.KeyPrefix + strconv.Itoa(i)
	}
	if n > 1 {
		p.krng = rand.New(rand.NewPCG(cfg.Seed, 0x2545f4914f6cdd1d))
		switch {
		case cfg.ZipfS == 0:
		case cfg.ZipfS > 1:
			p.zipf = rand.NewZipf(p.krng, cfg.ZipfS, 1, uint64(n-1))
		default:
			return nil, errors.New("zipf exponent must be above 1")
		}
	}
	return p, nil
}

func (p *picker) url(i int, at int64) *url.URL {
	if p.cum == nil {
		return p.urls[i%len(p.urls)]
	}
	cum := p.cum
	if p.shift > 0 && (at/p.shift)%2 == 1 {
		cum = p.rev
	}
	x := p.rng.Float64()
	for j, c := range cum {
		if x < c {
			return p.urls[j]
		}
	}
	return p.urls[len(p.urls)-1]
}

func (p *picker) key() string {
	switch {
	case p.keys == nil:
		return ""
	case p.krng == nil:
		return p.keys[0]
	case p.zipf != nil:
		return p.keys[p.zipf.Uint64()]
	}
	return p.keys[p.krng.IntN(len(p.keys))]
}

type Burst struct {
	Rate   float64       `json:"rate"`
	Length time.Duration `json:"length"`
	Every  time.Duration `json:"every"`
}
