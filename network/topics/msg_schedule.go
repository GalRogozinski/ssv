package topics

import (
	"fmt"
	"github.com/bloxapp/ssv-spec/qbft"
	"github.com/bloxapp/ssv-spec/types"
	"github.com/bloxapp/ssv/protocol/v2/qbft/roundtimer"
	ssvtypes "github.com/bloxapp/ssv/protocol/v2/types"
	"github.com/cornelk/hashmap"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.uber.org/zap"
	"oya.to/namedlocker"
	"sync"
	"time"
)

const (
	MaxDutyTypePerEpoch = 1
	Slot                = time.Second * 12
	Epoch               = Slot * 32
	// TODO this causes a timeout?
	// should be time between duties
	DecidedBeatDuration = time.Second * 2
	// If we see decided messages for the same instance enough times we may start invalidating
	// TODO express this as a function of faulty nodes in the cluster
	ProposeCountThreshold    = 1
	QbftSimilarMessagesSlack = 0
	RoundSlack               = 0
	NetLatency               = time.Millisecond * 1750
	// BetterOrSimilarMsgThreshold is the number of better or similar messages we need to see before we start rejecting.
	// Calculating the optimal bound is not as simple as it seems, so we just use some reasonably high number.
	BetterOrSimilarMsgThreshold = 50
)

// TODO: should seperate consensus mark and decided mark? This may take more memory but may remove the need for locks
type mark struct {
	rwLock sync.RWMutex

	//TODO after having production ready code add peermap solution
	//	peerMap *hashmap.Map[string]*mark

	//should be reset when a decided message is received with a larger height
	HighestRound    qbft.Round
	FirstMsgInRound *hashmap.Map[qbft.Round, time.Time]
	MsgTypesInRound *hashmap.Map[qbft.Round, *hashmap.Map[qbft.MessageType, int]]

	HighestDecided    qbft.Height
	DecidedRound      qbft.Round
	Last2DecidedTimes [2]time.Time
	MarkedDecided     int
	// the seen signers of the last decided message
	signers                 map[types.OperatorID]struct{}
	betterOrSimilarMsgCount int
}

func markID(id []byte) string {
	return fmt.Sprintf("%x", id)
}

func markLockID(id []byte, signer types.OperatorID) string {
	return fmt.Sprintf("%x-%d", id, signer)
}

// DurationFromLast2Decided returns duration for the last 3 decided including a new decided added at time.Now()
func (mark *mark) DurationFromLast2Decided() time.Duration {
	// If Last2DecidedTimes[1] is empty then it is fine to return a very long duration
	return time.Now().Sub(mark.Last2DecidedTimes[1])
}

func (mark *mark) addDecidedMark() {
	copy(mark.Last2DecidedTimes[1:], mark.Last2DecidedTimes[:1]) // shift array one up to make room at index 0
	mark.Last2DecidedTimes[0] = time.Now()

	mark.MarkedDecided++
}

// ResetForNewRound prepares round for new round.
// don't forget to lock mark before calling
func (mark *mark) ResetForNewRound(round qbft.Round, msgType qbft.MessageType) {
	mark.HighestRound = round
	if mark.FirstMsgInRound == nil {
		mark.FirstMsgInRound = hashmap.New[qbft.Round, time.Time]()
	}
	mark.FirstMsgInRound.Set(round, time.Now())
	if mark.MsgTypesInRound == nil {
		mark.MsgTypesInRound = hashmap.New[qbft.Round, *hashmap.Map[qbft.MessageType, int]]()
	}
	typesToOccurrences, exists := mark.MsgTypesInRound.Get(round)
	if exists {
		// we clear map instead of creating a new one to avoid memory allocation
		typesToOccurrences.Range(func(key qbft.MessageType, value int) bool {
			return typesToOccurrences.Del(key)
		})
	} else {
		typesToOccurrences = hashmap.New[qbft.MessageType, int]()
		mark.MsgTypesInRound.Insert(round, typesToOccurrences)
	}
	typesToOccurrences.Insert(msgType, 1)
}

func (mark *mark) updateDecidedMark(height qbft.Height, round qbft.Round, msgSignatures []types.OperatorID, plog *zap.Logger) {
	mark.rwLock.Lock()
	defer mark.rwLock.Unlock()

	if mark.HighestDecided > height {
		// In staging this condition never happened
		plog.Info("dropping an attempt to update decided mark", zap.Int("highestHeight", int(mark.HighestDecided)),
			zap.Int("height", int(height)))
		return
	}
	// TODO: Here we have an ugly hack to reset the mark for the first height
	// be aware it doesn't cause bugs
	if mark.HighestDecided < height || height == qbft.FirstHeight && mark.MarkedDecided == 0 {
		// reset for new height
		mark.HighestDecided = height
		mark.DecidedRound = round
		// maybe clear map instead of creating a new one
		mark.signers = make(map[types.OperatorID]struct{})
		mark.MarkedDecided = 0
		mark.HighestRound = qbft.NoRound
		mark.betterOrSimilarMsgCount = 0
		mark.FirstMsgInRound = hashmap.New[qbft.Round, time.Time]()
		// TODO: maybe we should clear the map instead of creating a new one..
		// However, clearing a large map may take a lot of time. Also no memory will ever be freed.. which may open an attack vector.
		mark.MsgTypesInRound = hashmap.New[qbft.Round, *hashmap.Map[qbft.MessageType, int]]()
		//TODO maybe clear current array. Probably yes, if 2 different duties come close together
		//mark.Last2DecidedTimes = [2]time.Time{}
	}

	// only update for highest decided
	if mark.HighestDecided == height {
		mark.addDecidedMark()
		for _, signer := range msgSignatures {
			// no need to insert if exists
			mark.signers[signer] = struct{}{}
		}
	}
}

// MessageSchedule keeps track of consensus msg schedules to determine timely receiving msgs
type MessageSchedule struct {

	//map id -> map signer -> mark
	Marks *hashmap.Map[string, *hashmap.Map[types.OperatorID, *mark]]
	sto   *namedlocker.Store
}

func NewMessageSchedule() *MessageSchedule {
	return &MessageSchedule{
		Marks: hashmap.New[string, *hashmap.Map[types.OperatorID, *mark]](),
		sto:   &namedlocker.Store{},
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
func (schedule *MessageSchedule) MarkConsensusMessage(signerMark *mark, identifier []byte, signer types.OperatorID, round qbft.Round, msgType qbft.MessageType, plog *zap.Logger) *mark {
	plog.Info("marking consensus message", zap.Int("msgType", int(msgType)), zap.Any("signer", signer))
	if signerMark == nil {
		signerMark = &mark{}
		schedule.setMark(markID(identifier), signer, signerMark)
	}
	signerMark.rwLock.Lock()
	defer signerMark.rwLock.Unlock()
	if signerMark.HighestRound < round {
		signerMark.ResetForNewRound(round, msgType)
	} else {
		occurrencesByType, exists := signerMark.MsgTypesInRound.Get(round)
		if !exists {
			// this wasn't called in staging
			plog.Warn("round missing from signer msgType Map")
			occurrencesByType = hashmap.New[qbft.MessageType, int]()
			occurrencesByType.Set(msgType, 1)
			signerMark.MsgTypesInRound.Set(round, occurrencesByType)
		} else {
			occurrences, exists := occurrencesByType.Get(msgType)
			plog.Debug("occurrences", zap.Int("occurrences", occurrences), zap.Bool("occured", exists))
			if !exists {
				occurrencesByType.Set(msgType, 1)
			} else {
				occurrencesByType.Set(msgType, occurrences+1)
			}
		}
	}
	return signerMark
}

func (schedule *MessageSchedule) findMark(id []byte, signer types.OperatorID) (*mark, bool) {
	idStr := markID(id)
	var signerMark *mark
	signerMark, found := schedule.getMark(idStr, signer)
	return signerMark, found
}

func (signerMark *mark) isConsensusMsgTimely(msg *qbft.SignedMessage, plog *zap.Logger, minRoundTime time.Time) (bool, pubsub.ValidationResult) {
	return signerMark.isConsensusMessageTimely(msg.Message.Height, msg.Message.Round, msg.Message.MsgType, msg.GetSigners(), minRoundTime, plog)
}

func (signerMark *mark) isConsensusMessageTimely(height qbft.Height, round qbft.Round, msgType qbft.MessageType, signers []types.OperatorID, minRoundTime time.Time, plog *zap.Logger) (bool, pubsub.ValidationResult) {
	signerMark.rwLock.RLock()
	defer signerMark.rwLock.RUnlock()
	plog = plog.With(zap.Any("markedRound", signerMark.HighestRound), zap.Any("markedHeight", signerMark.HighestDecided)).
		With(zap.Any("round", round))

	if signerMark.HighestDecided > height {
		plog.Warn("past height", zap.Any("height", height))
		return false, pubsub.ValidationReject
	} else if signerMark.HighestDecided == height {
		// if a commit message was received for a decided round, but we do not know about it, then we should process it
		// TODO there's an attack vector, someone can try to create commits for a decided round and we will process them
		// TODO check height == 0 case
		if msgType == qbft.CommitMsgType && signerMark.DecidedRound == round && signerMark.signers[signers[0]] != struct{}{} {
			plog.Warn("a commit message that should help create a better decided", zap.Any("height", height))
			// assume a late message and don't penalize
			// TODO maybe penalize every 2-3 messages?
			return true, pubsub.ValidationAccept
		}
		// TODO: ugly hack for height == 0 case
		// Problematic if another commit comes to make a better decided
		if height != 0 || signerMark.MarkedDecided > 0 {
			plog.Warn("a qbft message arrived for the same height was already decided", zap.Any("height", height))
			// assume a late message and don't penalize
			// TODO maybe penalize every 2-3 messages?
			return false, pubsub.ValidationIgnore
		}
		// TODO: Buggy Hack! Perhaps we can fork the network so height 0 is Bootstrap_Height or no Height!!
	} else if signerMark.HighestDecided != height-1 && signerMark.HighestDecided != 0 {
		plog.Warn("future height")
		// TODO assume that theres should be enough time between duties so that we don't see out of order messages
		// This is problematic in case we miss a decided message.
		// Maybe on the next decided message this will fix itself?
		return false, pubsub.ValidationReject
	}

	if signerMark.HighestRound < round {
		// if new round msg, check at least round timeout has passed
		//TODO research this time better - what happens with proposals timeout?
		// TODO: maybe inject function in a better way
		if time.Now().After(minRoundTime.Add(roundtimer.RoundTimeout(signerMark.HighestRound) - NetLatency)) {
			plog.Debug("timely round expiration", zap.Time("firstMsgInRound", minRoundTime))
			return true, pubsub.ValidationAccept
		} else {
			// TODO occurs in staging, why?
			plog.Warn("not timely round expiration", zap.Time("firstMsgInRound", minRoundTime))
			return false, pubsub.ValidationReject
		}
	} else if signerMark.HighestRound == round {
		// tooManyMessages check later on may change the result
		return true, pubsub.ValidationAccept
	} else {
		// past rounds are not timely
		// TODO: Important!!! Note: investigate what happens when a malicious signer signs a future round.
		// it may be fine since markers are per signer
		// one solution is to have threshold of signers update the marker's round. But are we guaranteed to see all the messages?
		plog.Warn("past round")
		// TODO research this slack. Consider using ValidationIgnore
		if round+RoundSlack >= signerMark.HighestRound {
			plog.Warn("past round but within slack", zap.Any("slack", RoundSlack))
			// TODO change to ValidationIgnore?
			return true, pubsub.ValidationAccept
		} else {
			return false, pubsub.ValidationReject
		}
	}
}

func (schedule *MessageSchedule) markDecidedMsg(signedMsg *qbft.SignedMessage, share *ssvtypes.SSVShare, plog *zap.Logger) {
	id := signedMsg.Message.Identifier
	var signers []types.OperatorID
	// All members of the committee should be marked. Even ones that didn't sign.
	// This is because non-signers should get marked so they move to the next height.
	for _, operator := range share.Committee {
		signer := operator.OperatorID
		signers = append(signers, signer)
	}
	height := signedMsg.Message.Height
	round := signedMsg.Message.Round

	schedule.markDecidedMessage(id, signers, height, round, signedMsg.Signers, plog)
}

func (schedule *MessageSchedule) markDecidedMessage(id []byte, committeeMembers []types.OperatorID, height qbft.Height, round qbft.Round, signatures []types.OperatorID, plog *zap.Logger) {
	for _, signer := range committeeMembers {
		idStr := markID(id)
		lockID := markLockID(id, signer)
		var signerMark *mark
		schedule.sto.Lock(lockID)
		defer schedule.sto.Unlock(lockID)
		signerMark, found := schedule.getMark(idStr, signer)
		if !found {
			signerMark = &mark{}
			schedule.setMark(idStr, signer, signerMark)
		}

		signerMark.updateDecidedMark(height, round, signatures, plog)
		plog.Info("decided mark updated", zap.Any("mark signer", signer), zap.Any("decided mark", signerMark))
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
	plog.Info("checking if decided message is timely")
	// TODO maybe we should check if all signers are in the committee (should be done in semantic checks)? - low priority
	//
	for _, signer := range signers {
		idStr := markID(id)
		var signerMark *mark
		signerMark, found := schedule.getMark(idStr, signer)
		if !found {
			// if one mark is missing it is enough to declare message as timely
			return true
		}

		signerMark.rwLock.RLock()
		defer signerMark.rwLock.RUnlock()

		plog.With(zap.Any("markedHeight", signerMark.HighestDecided))
		// TODO is this a problem when we sync?
		if signerMark.HighestDecided > height {
			plog.Warn("decided message is too old", zap.Int("markedHeight", int(signerMark.HighestDecided)))
			// maybe for other signers it is timely
			continue
		}
		if signerMark.HighestDecided == height {
			// This passed the hasBetterOrSimilarCheck so it is always timely
			//TODO maybe move betterOrSimilarMsgCount logic here?
			return true
		} else {
			fromLast2Decided := signerMark.DurationFromLast2Decided()
			plog.Warn("duration from last 2 decided",
				zap.Duration("duration", fromLast2Decided), zap.Any("markedHeight", signerMark.HighestDecided), zap.Any("signer", signer))
			// TODO research this time better
			if fromLast2Decided >= DecidedBeatDuration {
				return true
			}
		}
	}
	return false
}

func (schedule *MessageSchedule) minFirstMsgTimeForRound(signedMsg *qbft.SignedMessage, round qbft.Round, plogger *zap.Logger) time.Time {
	marks, _ := schedule.Marks.Get(markID(signedMsg.Message.Identifier))
	minRoundTime := time.Time{}
	marks.Range(func(signer types.OperatorID, mark *mark) bool {
		mark.rwLock.RLock()
		defer mark.rwLock.RUnlock()
		if mark.FirstMsgInRound == nil {
			plogger.Debug("first msg in round is nil", zap.Any("other signer", signer))
			return true
		}

		firstMsgInRoundTime, exists := mark.FirstMsgInRound.Get(round)
		if !exists {
			plogger.Debug("minFirstMsgTimeForRound - first msg in round is not found", zap.Any("other signer", signer))
			return true
		}

		if minRoundTime.IsZero() || firstMsgInRoundTime.Before(minRoundTime) {
			minRoundTime = firstMsgInRoundTime
		}
		return true
	})
	return minRoundTime
}

// TODO maybe similar messages should pass
// hasBetterOrSimilarMsg returns true if there is a decided message with the same height and the same or more signatures
func (schedule *MessageSchedule) hasBetterOrSimilarMsg(commit *qbft.SignedMessage) (bool, pubsub.ValidationResult) {
	idStr := markID(commit.Message.Identifier)

	signerMap, found := schedule.Marks.Get(idStr)
	if !found {
		return false, pubsub.ValidationAccept
	}

	validation := pubsub.ValidationAccept
	betterOrSimilarMsg := false
	signerMap.Range(func(key types.OperatorID, signerMark *mark) bool {
		signerMark.rwLock.RLock()
		defer signerMark.rwLock.RUnlock()
		// Even if signers are different, we assume that due to propagating commits we will see the better decided messages later
		if signerMark.HighestDecided == commit.Message.Height && len(commit.Signers) <= len(signerMark.signers) {
			betterOrSimilarMsg = true
			if signerMark.betterOrSimilarMsgCount >= BetterOrSimilarMsgThreshold {
				validation = pubsub.ValidationReject
				return false
			}
			// continue to check other signers, maybe they will be rejected
			// TODO maybe we don't need this constant per signer?
			signerMark.betterOrSimilarMsgCount++
			validation = pubsub.ValidationIgnore
			return true
		}
		return true
	})

	return betterOrSimilarMsg, validation
}

func (signerMark *mark) tooManyMessagesPerRound(msg *qbft.SignedMessage, share *ssvtypes.SSVShare, plog *zap.Logger) bool {
	round := msg.Message.Round
	msgType := msg.Message.MsgType

	return signerMark.tooManyMsgsPerRound(round, msgType, share, plog)
}

func (signerMark *mark) tooManyMsgsPerRound(round qbft.Round, msgType qbft.MessageType, share *ssvtypes.SSVShare, plog *zap.Logger) bool {
	signerMark.rwLock.RLock()
	defer signerMark.rwLock.RUnlock()
	plog.Info("checking if too many messages per round", zap.Any("round", round), zap.Any("msgType", msgType))
	if signerMark.MsgTypesInRound == nil {
		signerMark.MsgTypesInRound = hashmap.New[qbft.Round, *hashmap.Map[qbft.MessageType, int]]()
	}
	msgTypeToOccurrences, found := signerMark.MsgTypesInRound.Get(round)
	if !found {
		plog.Info("mark was not initialized for round")
		// must not initialize inner map here because memory allocation should happen only after signature validation
		return false
	}
	occurrence, exists := msgTypeToOccurrences.Get(msgType)

	if !exists {
		return false
	}

	switch msgType {
	//should always be 1
	case qbft.ProposalMsgType:
		// TODO this case occured in staging even though it shouldn't
		return occurrence >= ProposeCountThreshold+QbftSimilarMessagesSlack
	default:
		return occurrence >= len(share.Committee)+QbftSimilarMessagesSlack
	}
}
