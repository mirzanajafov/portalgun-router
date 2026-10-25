package limit

import (
	"math/rand/v2"
	"strconv"
	"time"
)

const (
	forgetDebt = time.Minute
	debtTTL    = 10 * time.Minute
)

type debt struct {
	epoch string
	total float64
	paid  float64
	at    int64
}

func (d *debt) add(n float64, now int64) {
	if n <= 0 {
		return
	}
	if d.epoch == "" {
		d.epoch = strconv.FormatUint(rand.Uint64(), 36)
	}
	d.total += n
	d.at = now
}

func (d *debt) unpaid() (string, float64) {
	if d.total > d.paid {
		return d.epoch, d.total
	}
	return "", 0
}

func (d *debt) settle(epoch string, total float64) {
	if epoch == "" || epoch != d.epoch {
		return
	}
	d.paid = max(d.paid, total)
	if d.paid >= d.total {
		d.epoch, d.total, d.paid = "", 0, 0
	}
}

func (d *debt) clear() bool { return d.epoch == "" }

func debtKeys(key, epoch string) []string {
	if epoch == "" {
		return []string{key}
	}
	return []string{key, key + ":debt:" + epoch}
}

type ledger struct {
	debts shards[debt]
}

func (l *ledger) add(key string, n float64, now int64) {
	sh := l.debts.of(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	d, ok := sh.m[key]
	if !ok {
		d = &debt{}
		sh.add(key, d, func(old *debt) bool { return now-old.at >= int64(forgetDebt) })
	}
	d.add(n, now)
}

func (l *ledger) unpaid(key string) (string, float64) {
	sh := l.debts.of(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if d, ok := sh.m[key]; ok {
		return d.unpaid()
	}
	return "", 0
}

func (l *ledger) settle(key, epoch string, total float64) {
	if epoch == "" {
		return
	}
	sh := l.debts.of(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if d, ok := sh.m[key]; ok {
		d.settle(epoch, total)
		if d.clear() {
			delete(sh.m, key)
		}
	}
}

func (l *ledger) size() int { return l.debts.size() }
