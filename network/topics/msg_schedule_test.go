package topics

import (
	"github.com/bloxapp/ssv-spec/qbft"
	"github.com/bloxapp/ssv-spec/types"
	"github.com/bloxapp/ssv/protocol/v2/qbft/roundtimer"
	ssvtypes "github.com/bloxapp/ssv/protocol/v2/types"
	"github.com/cornelk/hashmap"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/stretchr/testify/require"
	zap "go.uber.org/zap"
	"testing"
	"time"
)

var (
	decidedMsgId = []byte{1, 2, 3}
	qbftMsgId    = []byte{1, 2, 3, 4}
)

// TestMarkConsensusMessage tests marking a consensus message.
func TestMarkConsensusMessage(t *testing.T) {
	// Test given a nil mark, a new mark is created.
	s := NewMessageSchedule()
	signerMark := s.MarkConsensusMessage(nil, qbftMsgId, 0, qbft.FirstRound, qbft.PrepareMsgType, zap.L())
	require.NotNil(t, signerMark)
	firstMsgTime, exists := signerMark.FirstMsgInRound.Get(qbft.FirstRound)
	require.True(t, exists, "firstMsgTime should exist")
	require.True(t, time.Now().Sub(firstMsgTime) < time.Second, "FirstMsgInRound should be set to now")
	require.Equal(t, qbft.FirstRound, signerMark.HighestRound, "highest round should be FirstRound")
	messageToOccurenceMap, exists := signerMark.MsgTypesInRound.Get(qbft.FirstRound)
	require.True(t, exists, "messageToOccurenceMap should exist")
	occurence, exists := messageToOccurenceMap.Get(qbft.PrepareMsgType)
	require.True(t, exists, "occurence should exist")
	require.Equal(t, 1, occurence, "occurence should be 1")
	// test FirstMsgInRound Time is set correctly.

	// Test given a non-nil mark, the mark is updated.
	// Create a non-nil mark. With a higher round.
	signerMark = s.MarkConsensusMessage(signerMark, qbftMsgId, 0, qbft.Round(2), qbft.PrepareMsgType, zap.L())
	require.NotNil(t, signerMark)
	firstMsgTime, exists = signerMark.FirstMsgInRound.Get(qbft.Round(2))
	require.True(t, exists, "firstMsgTime should exist")
	require.True(t, time.Now().Sub(firstMsgTime) < time.Second, "FirstMsgInRound should be set to now")
	// Test that marks fields have the expected values.
	require.Equal(t, qbft.Round(2), signerMark.HighestRound, "highest round should be FirstRound")
	messageToOccurenceMap, exists = signerMark.MsgTypesInRound.Get(qbft.Round(2))
	require.True(t, exists, "messageToOccurenceMap should exist")
	occurence, exists = messageToOccurenceMap.Get(qbft.PrepareMsgType)
	require.True(t, exists, "occurence should exist")
}

// TestMarkDecidedMessage tests marking a decided message.

func TestMarkDecidedMessage(t *testing.T) {
	s := NewMessageSchedule()
	// mark decided message with several operator ids.
	operatorIDs := []types.OperatorID{0, 1, 2}
	s.markDecidedMessage(decidedMsgId, operatorIDs, 1, 0, []types.OperatorID{0, 1, 2}, zap.L())
	// test that scheduler has a mark for each operator id.
	id := markID(decidedMsgId)
	for _, operatorID := range operatorIDs {
		signerMark, exists := s.getMark(id, operatorID)
		require.True(t, exists, "decidedMessages should contain a mark for operator id %d", operatorID)
		// test that HighestDecided is correct
		require.Equal(t, qbft.Height(1), signerMark.HighestDecided, "HighestDecided should be 1")
		// test that MarkedDecided is correct
		require.Equal(t, 1, signerMark.MarkedDecided, "MarkedDecided should be 1")
		// test that signers is correct
		require.ElementsMatch(t, []types.OperatorID{0, 1, 2}, extractKeysFromMap(signerMark.signers))
		// test that rest of signerMark fields are correct
		require.Equal(t, qbft.NoRound, signerMark.HighestRound, "HighestRound should be FirstRound")
		_, exists = signerMark.FirstMsgInRound.Get(qbft.NoRound)
		require.False(t, exists, "FirstMsgInRound time should not be set")
		// test that the first last2DecidedTime is close to now
		require.True(t, time.Now().Sub(signerMark.Last2DecidedTimes[0]) < time.Second, "last2DecidedTime should be set to now")
	}

	// test that decided count is updated
	s.markDecidedMessage(decidedMsgId, operatorIDs, 1, 0, []types.OperatorID{0, 1}, zap.L())
	for _, operatorID := range operatorIDs {
		signerMark, _ := s.getMark(id, operatorID)
		require.Equal(t, 2, signerMark.MarkedDecided, "MarkedDecided should be 2")
		require.ElementsMatch(t, []types.OperatorID{0, 1, 2}, extractKeysFromMap(signerMark.signers), "signers can't be removed")
		// test that last2DecidedTime is updated
		require.True(t, time.Now().Sub(signerMark.Last2DecidedTimes[1]) < time.Second, "last2DecidedTime should be set to now")
	}
	// old height
	s.markDecidedMessage(decidedMsgId, []types.OperatorID{0, 1}, 0, 0, []types.OperatorID{0, 1, 2, 3, 4}, zap.L())
	for _, operatorID := range operatorIDs {
		signerMark, _ := s.getMark(id, operatorID)
		require.Equal(t, qbft.Height(1), signerMark.HighestDecided, "HighestDecided should be 1")
		require.Equal(t, 2, signerMark.MarkedDecided, "MarkedDecided should be 2, since we don't update old height")
		require.ElementsMatch(t, []types.OperatorID{0, 1, 2}, extractKeysFromMap(signerMark.signers), "numOfSignatures should be 3, since we don't update old height")
	}
	// new height
	s.markDecidedMessage(decidedMsgId, operatorIDs, 2, 0, []types.OperatorID{0, 1}, zap.L())
	for _, operatorID := range operatorIDs {
		signerMark, _ := s.getMark(id, operatorID)
		require.Equal(t, qbft.Height(2), signerMark.HighestDecided, "HighestDecided should be 2")
		require.Equal(t, 1, signerMark.MarkedDecided, "MarkedDecided should be 1")
		require.ElementsMatch(t, []types.OperatorID{0, 1}, extractKeysFromMap(signerMark.signers))
	}
}

func TestIsTimelyDecidedMessage(t *testing.T) {
	// Test given a nil mark, the message is not timely.
	s := NewMessageSchedule()

	isTimely := s.isTimelyDecidedMessage(decidedMsgId, []types.OperatorID{0, 1, 2}, qbft.FirstHeight, zap.L())
	require.True(t, isTimely, "message should be timely because it has no mark")
	// Test given a non-nil mark, the message is timely.
	// Create a non-nil mark. With a higher round.
	s.markDecidedMessage(decidedMsgId, []types.OperatorID{0, 1, 2, 3}, qbft.FirstHeight, 0, []types.OperatorID{0, 1, 2}, zap.L())
	isTimely = s.isTimelyDecidedMessage(decidedMsgId, []types.OperatorID{0, 1, 2}, qbft.FirstHeight, zap.L())
	require.True(t, isTimely, "message should be timely because it is of last height")
	s.markDecidedMessage(decidedMsgId, []types.OperatorID{0, 1, 2, 3}, qbft.Height(2), 0, []types.OperatorID{0, 1, 2}, zap.L())
	isTimely = s.isTimelyDecidedMessage(decidedMsgId, []types.OperatorID{1, 2, 3}, qbft.Height(3), zap.L())
	require.False(t, isTimely, "message shouldn't be timely because not enough time has passed")
	mark, found := s.findMark(decidedMsgId, 0)
	require.True(t, found, "mark should exist")

	// change last2DecidedTimes to be 2 seconds ago.
	mark.Last2DecidedTimes[1] = time.Now().Add(-2 * time.Second)
	isTimely = s.isTimelyDecidedMessage(decidedMsgId, []types.OperatorID{0, 1, 2}, qbft.Height(3), zap.L())
	require.True(t, isTimely, "message should be timely because enough time has passed")
}

func TestHasBetterMsg(t *testing.T) {
	// Test given a nil mark, the message is better.
	s := NewMessageSchedule()
	signedMsg := &qbft.SignedMessage{
		Signers: []types.OperatorID{0, 1, 2},
		Message: qbft.Message{
			MsgType:                  qbft.CommitMsgType,
			Height:                   qbft.FirstHeight,
			Round:                    qbft.FirstRound,
			Identifier:               decidedMsgId,
			Root:                     [32]byte{},
			DataRound:                1,
			RoundChangeJustification: nil,
			PrepareJustification:     nil,
		},
	}
	betterOrSimilarMsg, result := s.hasBetterOrSimilarMsg(signedMsg)
	require.False(t, betterOrSimilarMsg)
	require.Equal(t, pubsub.ValidationAccept, result)
	s.markDecidedMessage(decidedMsgId, []types.OperatorID{1, 2, 3}, qbft.FirstHeight, 0, []types.OperatorID{0, 1, 2}, zap.L())
	betterOrSimilarMsg, result = s.hasBetterOrSimilarMsg(signedMsg)
	require.True(t, betterOrSimilarMsg)
	require.Equal(t, pubsub.ValidationIgnore, result)
}

// TestIsConsensusMessageTimely tests isConsensusMessageTimely.
func TestIsConsensusMessageTimely(t *testing.T) {
	signerMark := &mark{
		HighestRound:      2,
		FirstMsgInRound:   hashmap.New[qbft.Round, time.Time](),
		MsgTypesInRound:   nil,
		HighestDecided:    2,
		Last2DecidedTimes: [2]time.Time{},
		MarkedDecided:     0,
		signers:           map[types.OperatorID]struct{}{},
	}
	// test that message is not timely if it has height smaller than highestDecided.
	isTimely, validationResult := signerMark.isConsensusMessageTimely(qbft.Height(1), qbft.Round(3), 0, nil, time.Time{}, zap.L())
	require.False(t, isTimely, "message shouldn't be timely because it has height smaller than highestDecided")
	require.Equal(t, pubsub.ValidationReject, validationResult, "validation result should be reject")

	// test that message is not timely if it has height equal to highestDecided
	isTimely, validationResult = signerMark.isConsensusMessageTimely(qbft.Height(2), qbft.Round(3), 0, nil, time.Time{}, zap.L())
	require.False(t, isTimely, "message shouldn't be timely because it has height equal to highestDecided")
	require.Equal(t, pubsub.ValidationIgnore, validationResult, "validation result should be ignore")

	// test that message is not timely if it has height too much above highestDecided
	isTimely, validationResult = signerMark.isConsensusMessageTimely(qbft.Height(4), qbft.Round(3), 0, nil, time.Time{}, zap.L())
	require.False(t, isTimely, "message shouldn't be timely because it has height too much above highest decided highestDecided")
	require.Equal(t, pubsub.ValidationReject, validationResult, "validation result should be reject")

	// test that message is timely if it has round smaller than highestRound.
	isTimely, validationResult = signerMark.isConsensusMessageTimely(qbft.Height(3), qbft.Round(1), 0, nil, time.Time{}, zap.L())
	require.False(t, isTimely, "message shouldn't be timely because it has round smaller than highestime.Now()tDecided")
	require.Equal(t, pubsub.ValidationReject, validationResult, "validation result should be reject")
	// test that message is timely if it has round larger than highestRound
	// first set firstMsgInRound to now.
	isTimely, validationResult = signerMark.isConsensusMessageTimely(qbft.Height(3), qbft.Round(3), 0, nil, time.Now(), zap.L())
	// should fail because new round came too soon.
	require.False(t, isTimely, "message shouldn't be timely because it came too soon")
	require.Equal(t, pubsub.ValidationReject, validationResult, "validation result should be reject")
	// change firstMsgInRound
	signerMark.FirstMsgInRound.Set(qbft.Round(2), time.Now().Add(-roundtimer.RoundTimeout(3)).Add(-NetLatency))
	isTimely, validationResult = signerMark.isConsensusMessageTimely(qbft.Height(3), qbft.Round(3), 0, nil, time.Now().Add(-roundtimer.RoundTimeout(3)).Add(-NetLatency), zap.L())
	// should pass because new round came after timeout.
	require.True(t, isTimely, "message should be timely because it came after timeout")
	require.Equal(t, pubsub.ValidationAccept, validationResult, "validation result should be accept")
}

func TestTooManyMsgsPerRound(t *testing.T) {
	// test empty mark
	signerMark := &mark{
		MsgTypesInRound: hashmap.New[qbft.Round, *hashmap.Map[qbft.MessageType, int]](),
	}
	// create a committee of 4 operators.
	share := &ssvtypes.SSVShare{Share: types.Share{Committee: []*types.Operator{{}, {}, {}, {}}}}
	tooManyMsgs := signerMark.tooManyMsgsPerRound(qbft.FirstRound, qbft.PrepareMsgType, share, zap.L())
	require.False(t, tooManyMsgs, "shouldn't be too many messages because mark is empty")
	// test mark with 2 messages of type prepare in first round.
	_, found := signerMark.MsgTypesInRound.Get(qbft.FirstRound)
	require.False(t, found, "Inner map shouldn't be allocated to prevent memory attack")

	occurences := hashmap.New[qbft.MessageType, int]()
	signerMark.MsgTypesInRound.Set(qbft.FirstRound, occurences)

	tooManyMsgs = signerMark.tooManyMsgsPerRound(qbft.FirstRound, qbft.ProposalMsgType, share, zap.L())
	require.False(t, tooManyMsgs, "shouldn't be too many messages because there are 0 messages of type prepare in first round")
	// set 1 message of type propose in first round.
	occurences.Set(qbft.ProposalMsgType, 1)
	tooManyMsgs = signerMark.tooManyMsgsPerRound(qbft.FirstRound, qbft.ProposalMsgType, share, zap.L())
	require.True(t, tooManyMsgs, "there can be at most one propose message")

	// test commit and prepare messages
	tooManyMsgs = signerMark.tooManyMsgsPerRound(qbft.FirstRound, qbft.PrepareMsgType, share, zap.L())
	occurences.Set(qbft.PrepareMsgType, 2)
	tooManyMsgs = signerMark.tooManyMsgsPerRound(qbft.FirstRound, qbft.PrepareMsgType, share, zap.L())
	require.False(t, tooManyMsgs, "shouldn't be too many messages because there are 2 messages of type prepare in first round")
	// test mark with 3 messages of type commit in first round.
	occurences.Set(qbft.CommitMsgType, 3)
	tooManyMsgs = signerMark.tooManyMsgsPerRound(qbft.FirstRound, qbft.CommitMsgType, share, zap.L())
	require.False(t, tooManyMsgs, "shouldn't be too many messages because there are 3 messages of type commit in first round")
	// test mark with 4 messages of type prepare in first round.
	occurences.Set(qbft.PrepareMsgType, 4)
	tooManyMsgs = signerMark.tooManyMsgsPerRound(qbft.FirstRound, qbft.PrepareMsgType, share, zap.L())
	require.True(t, tooManyMsgs, "should be too many messages because there are 4 messages of type prepare in first round")
	// test mark with 4 messages of type commit in first round.
	occurences.Set(qbft.CommitMsgType, 4)
	tooManyMsgs = signerMark.tooManyMsgsPerRound(qbft.FirstRound, qbft.CommitMsgType, share, zap.L())
	require.True(t, tooManyMsgs, "should be too many messages because there are 4 messages of type commit in first round")
}

func extractKeysFromMap[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
