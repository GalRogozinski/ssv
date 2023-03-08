package topics

import (
	"fmt"
	"github.com/bloxapp/ssv-spec/qbft"
	"github.com/bloxapp/ssv-spec/types"
	"github.com/cornelk/hashmap"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	MaxDutyTypePerEpoch = 1
	Slot                = time.Second * 12
	Epoch               = Slot * 32
	//TODO this causes a timeout
	// should be time between duties
	DecidedBeatDuration = time.Second * 2
	// If we see decided messages for the same instance enough times we may start invalidating
	DecidedCountThreshold = 2
)

// RoundTimeout returns timeout duration for round
var RoundTimeout = func(round qbft.Round) time.Duration {
	return time.Second * 2
}

// TODO: seperate consensus mark and decided mark
type mark struct {
	rwLock sync.RWMutex

	//should be reset when a decided message is received with a larger height
	HighestRound    qbft.Round
	FirstMsgInRound time.Time
	MsgTypesInRound map[qbft.MessageType]bool

	HighestDecided    qbft.Height
	Last2DecidedTimes [2]time.Time
	MarkedDecided     int
	numOfSignatures   int
}

func markID(id []byte) string {
	return fmt.Sprintf("%x", id)
}

// DurationFromLast2Decided returns duration for the last 3 decided including a new decided added at time.Now()
func (mark *mark) DurationFromLast2Decided() time.Duration {
	return time.Now().Sub(mark.Last2DecidedTimes[1])
}

// TODO fix shift?
func (mark *mark) AddDecidedMark() {
	copy(mark.Last2DecidedTimes[1:], mark.Last2DecidedTimes[:1]) // shift array one up to make room at index 0
	mark.Last2DecidedTimes[0] = time.Now()

	mark.MarkedDecided++
}

func (mark *mark) ResetForNewRound(round qbft.Round, msgType qbft.MessageType) {
	mark.HighestRound = round
	mark.FirstMsgInRound = time.Now()
	// TODO this may put strain on GC, maybe better to clear existing map
	mark.MsgTypesInRound = map[qbft.MessageType]bool{
		msgType: true,
	}
}

// should lock before calling
func (mark *mark) updateDecidedMark(height qbft.Height, signers []types.OperatorID, plog *zap.Logger) {
	if mark.HighestDecided > height {
		plog.Info("dropping an attempt to update decided mark", zap.Int("highestHeight", int(mark.HighestDecided)),
			zap.Int("height", int(height)))
		return
	}
	mark.rwLock.Lock()
	defer mark.rwLock.Unlock()
	if mark.HighestDecided < height {
		// reset for new height
		mark.HighestDecided = height
		mark.MarkedDecided = 1
		//mark.HighestRound = qbft.NoRound
		//TODO maybe clear current array
		//mark.Last2DecidedTimes = [2]time.Time{}
	}
	mark.AddDecidedMark()
	//must be a better decided message, but we check anyhow
	if mark.numOfSignatures < len(signers) {
		mark.numOfSignatures = len(signers)
	}
}

// MessageSchedule keeps track of consensus msg schedules to determine timely receiving msgs
type MessageSchedule struct {
	//map id -> map signer -> mark
	Marks *hashmap.Map[string, *hashmap.Map[types.OperatorID, *mark]]
}

func NewMessageSchedule() *MessageSchedule {
	return &MessageSchedule{
		Marks: hashmap.New[string, *hashmap.Map[types.OperatorID, *mark]](),
	}
}

func (schedule *MessageSchedule) getMark(id string, signer types.OperatorID) (*mark, bool) {
	marksBySigner, found := schedule.Marks.Get(id)
	if !found {
		return nil, false
	}
	return marksBySigner.Get(signer)
}

// sets mark for message Identifier ans signer
func (schedule *MessageSchedule) setMark(id string, signer types.OperatorID, newMark *mark) {
	marksBySigner, found := schedule.Marks.Get(id)
	if !found {
		marksBySigner = hashmap.New[types.OperatorID, *mark]()
		schedule.Marks.Set(id, marksBySigner)
	}
	marksBySigner.Set(signer, newMark)
}

// MarkConsensusMessage marks a msg
func (schedule *MessageSchedule) MarkConsensusMessage(id []byte, signer types.OperatorID, round qbft.Round, msgType qbft.MessageType) {
	idStr := markID(id)
	var signerMark *mark
	signerMark, found := schedule.getMark(idStr, signer)
	if !found {
		signerMark = &mark{}
		schedule.setMark(idStr, signer, signerMark)
	}

	if signerMark.HighestRound < round {
		signerMark.ResetForNewRound(round, msgType)
	} else {
		signerMark.MsgTypesInRound[msgType] = true
	}
}

func (schedule *MessageSchedule) isConsensusMsgTimely(msg *qbft.SignedMessage, plog *zap.Logger) bool {
	return schedule.isConsensusMessageTimely(msg.Message.Identifier, msg.Signers[0], msg.Message.Round, msg.Message.MsgType, plog)
}

func (schedule *MessageSchedule) isConsensusMessageTimely(id []byte, signer types.OperatorID, round qbft.Round, msgType qbft.MessageType, plog *zap.Logger) bool {
	idStr := markID(id)
	var signerMark *mark
	signerMark, found := schedule.getMark(idStr, signer)
	if !found {
		return true
	}

	plog.With(zap.Any("round", round), zap.Any("highestRound", signerMark.HighestRound))
	if signerMark.HighestRound < round {
		// if new round msg, check at least round timeout has passed
		//TODO research this time better
		if time.Now().After(signerMark.FirstMsgInRound.Add(RoundTimeout(signerMark.HighestRound))) {
			plog.Debug("timely round expiration", zap.Time("firstMsgInRound", signerMark.FirstMsgInRound))
			return true
		} else {
			plog.Warn("not timely round expiration", zap.Time("firstMsgInRound", signerMark.FirstMsgInRound))
			return false
		}
	} else if signerMark.HighestRound == round {
		// if in current round we saw messages of this type then this is bad
		//TODO for prepare/commit messages this is bad
		_, found := signerMark.MsgTypesInRound[msgType]
		return !found
	} else {
		// past rounds are not timely
		// TODO this may be too strict
		plog.Warn("past round", zap.Any("round", round), zap.Any("highestRound", signerMark.HighestRound))
		return false
	}
}

// TODO clean up marks to protect from memory DOS attacks
func (schedule *MessageSchedule) markDecidedMsg(signedMsg *qbft.SignedMessage, plog *zap.Logger) {
	id := signedMsg.Message.Identifier
	signers := signedMsg.Signers
	height := signedMsg.Message.Height

	schedule.markDecidedMessage(id, signers, height, plog)
}

func (schedule *MessageSchedule) markDecidedMessage(id []byte, signers []types.OperatorID, height qbft.Height, plog *zap.Logger) {
	for _, signer := range signers {
		idStr := markID(id)
		var signerMark *mark
		signerMark, found := schedule.getMark(idStr, signer)
		if !found {
			signerMark = &mark{}
			schedule.setMark(idStr, signer, signerMark)
		}
		signerMark.updateDecidedMark(height, signers, plog)
		plog.Info("decided mark updated", zap.Any("decided mark", signerMark))
	}
}

// isTimelyDecidedMsg returns true if decided message is timely (both for future and past decided messages)
// FUTURE: when a valid decided msg is received, the next duty for the runner is marked. The next decided message will not be validated before that time.
// Aims to prevent a byzantine committee rapidly broadcasting decided messages
// PAST: a decided message which is "too" old will be rejected as well
func (schedule *MessageSchedule) isTimelyDecidedMsg(msg *qbft.SignedMessage, plog *zap.Logger) bool {
	return schedule.isTimelyDecidedMessage(msg.Message.Identifier, msg.Signers, msg.Message.Height, plog)
}

func (schedule *MessageSchedule) isTimelyDecidedMessage(id []byte, signers []types.OperatorID, height qbft.Height, plog *zap.Logger) bool {
	ret := false
	plog.Info("checking if decided message is timely")
	for _, signer := range signers {
		idStr := markID(id)
		var signerMark *mark
		signerMark, found := schedule.getMark(idStr, signer)
		if !found {
			ret = true
			continue
		}

		// TODO is this a problem when we sync?

		if signerMark.HighestDecided > height {
			plog.Warn("decided message is too old", zap.Int("current height", int(height)), zap.Int("highest decided", int(signerMark.HighestDecided)))
			return false
		}

		if signerMark.MarkedDecided >= DecidedCountThreshold {
			fromLast2Decided := signerMark.DurationFromLast2Decided()
			plog.Warn("duration from last 2 decided",
				zap.Duration("duration", fromLast2Decided), zap.Any("markedHeight", signerMark.HighestDecided))
			return fromLast2Decided >= DecidedBeatDuration
		} else {
			ret = true
		}
	}
	return ret
}

// checks if there is a better message for that instance
func (schedule *MessageSchedule) hasBetterMsg(commit *qbft.SignedMessage) bool {
	idStr := markID(commit.Message.Identifier)

	signerMap, found := schedule.Marks.Get(idStr)
	if !found {
		return false
	}

	betterMsg := false
	signerMap.Range(func(key types.OperatorID, signerMark *mark) bool {
		if signerMark.HighestDecided == commit.Message.Height && len(commit.Signers) < signerMark.numOfSignatures {
			betterMsg = true
			return false
		}
		return true
	})

	return betterMsg
}
