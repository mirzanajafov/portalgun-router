package sim

func Grid(aaTrials, trials int) []Cell {
	var cells []Cell
	for _, base := range []float64{0.001, 0.005, 0.02} {
		for _, share := range []float64{0.01, 0.05, 0.25} {
			for _, rps := range []float64{200, 2000} {
				for _, blip := range []bool{false, true} {
					cells = append(cells, Cell{Group: "a/a", BaseError: base, Share: share, Rho: 1, RPS: rps, Blip: blip, Trials: aaTrials})
				}
			}
		}
	}
	for _, base := range []float64{0.001, 0.005, 0.02} {
		for _, rps := range []float64{200, 2000} {
			for _, rho := range []float64{1.2, 1.5, 2, 5} {
				cells = append(cells, Cell{Group: "errors", BaseError: base, Share: 0.05, Rho: rho, RPS: rps, Trials: trials})
			}
			cells = append(cells, Cell{Group: "errors", BaseError: base, Share: 0.05, Rho: 1, TotalFailure: true, RPS: rps, Trials: trials})
		}
	}
	for _, share := range []float64{0.01, 0.25} {
		cells = append(cells, Cell{Group: "share", BaseError: 0.005, Share: share, Rho: 2, RPS: 2000, Trials: trials})
	}
	for _, rps := range []float64{200, 2000} {
		for _, slow := range []float64{0.10, 0.99} {
			cells = append(cells, Cell{Group: "latency", BaseError: 0.005, Share: 0.05, Rho: 1, CanarySlow: slow, RPS: rps, Trials: trials})
		}
	}
	return cells
}
