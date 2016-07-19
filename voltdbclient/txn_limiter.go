/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package voltdbclient

import (
	"errors"
	"math"
	"sync"
	"time"
)

const (
	BLOCK_DURATION = time.Millisecond * 100
)

type txnLimiter struct {
	blockStart time.Time
	txnCount   int
	// the number of transactions per block
	targetTxnCount int
	mutex          sync.RWMutex
}

func newTxnLimiter() *txnLimiter {
	var tl = new(txnLimiter)
	tl.blockStart = time.Now()
	tl.txnCount = 0
	tl.targetTxnCount = math.MaxInt32
	return tl
}

func (tl *txnLimiter) setTxnsPerSecond(txnPS int) {
	tl.targetTxnCount = int(math.Ceil(float64(txnPS) / float64(10)))
}

// interface for rateLimiter
func (tl *txnLimiter) limit(timeout time.Duration) error {
	start := time.Now()
	for !tl.permit() {
		time.Sleep(time.Millisecond)
		if time.Since(start).Nanoseconds() > timeout.Nanoseconds() {
			return errors.New("timeout")
		}
	}
	return nil
}

// interface for rateLimiter
func (tl *txnLimiter) responseReceived(latency int32) {
	tl.mutex.Lock()
	tl.txnCount -= 1
	tl.mutex.Unlock()
}

func (tl *txnLimiter) nextBlock() bool {
	var nextBlock bool = false
	for time.Since(tl.blockStart).Nanoseconds() > BLOCK_DURATION.Nanoseconds() {
		tl.blockStart = tl.blockStart.Add(BLOCK_DURATION)
		tl.txnCount = 0
		nextBlock = true
	}
	return nextBlock
}

// returns true if the transaction can proceed.  increments the transaction count.
func (tl *txnLimiter) permit() bool {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()
	for tl.nextBlock() {
	}
	now := time.Now()
	elapsed := now.Sub(tl.blockStart)
	percentElapsed := float64(elapsed.Nanoseconds()) / float64(BLOCK_DURATION.Nanoseconds())
	if tl.txnCount > 0 {
		txnsAllowed := int(math.Ceil(float64(tl.targetTxnCount) * percentElapsed))
		if tl.txnCount > txnsAllowed {
			return false
		}
	}
	tl.txnCount++
	return true
}
