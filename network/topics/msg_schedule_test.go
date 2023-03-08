package topics

import (
	"github.com/bloxapp/ssv-spec/qbft"
	"github.com/bloxapp/ssv-spec/types"
	"github.com/stretchr/testify/require"
	zap "go.uber.org/zap"
	"testing"
	"time"
)

func TestMessageSchedule_MarkConsensusMessage(t *testing.T) {
	s := NewMessageSchedule()
	s.MarkConsensusMessage([]byte{1, 2, 3, 4}, 0, qbft.FirstRound, qbft.PrepareMsgType)
	require.False(t, s.isConsensusMessageTimely([]byte{1, 2, 3, 4}, 0, qbft.FirstRound, qbft.PrepareMsgType, zap.L()))
	require.True(t, s.isConsensusMessageTimely([]byte{1, 2, 3, 4}, 0, qbft.FirstRound, qbft.CommitMsgType, zap.L()))

	require.False(t, s.isConsensusMessageTimely([]byte{1, 2, 3, 4}, 0, 2, qbft.PrepareMsgType, zap.L()))
	<-time.After(time.Second * 2)
	require.True(t, s.isConsensusMessageTimely([]byte{1, 2, 3, 4}, 0, 2, qbft.PrepareMsgType, zap.L()))

	s.markDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 1, zap.L())
	require.True(t, s.isConsensusMessageTimely([]byte{1, 2, 3, 4}, 0, qbft.FirstRound, qbft.PrepareMsgType, zap.L()))
}

func TestMessageSchedule_MarkDecidedMessage(t *testing.T) {
	s := NewMessageSchedule()
	require.True(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 1, zap.L()))
	s.markDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 1, zap.L())
	require.False(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 1, zap.L()))

	require.True(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 2, zap.L()))
	s.markDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 2, zap.L())
	require.False(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 2, zap.L()))

	require.False(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 3, zap.L()))
	<-time.After(time.Second * 2)
	require.True(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 3, zap.L()))
	s.markDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 3, zap.L())
	require.False(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 3, zap.L()))

	require.True(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 4, zap.L()))
	s.markDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 4, zap.L())
	require.False(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 4, zap.L()))

	require.False(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 5, zap.L()))
	<-time.After(time.Second * 2)
	require.True(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 5, zap.L()))
	s.markDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 5, zap.L())
	require.False(t, s.isTimelyDecidedMessage([]byte{1, 2, 3, 4}, []types.OperatorID{0, 1, 2}, 5, zap.L()))
}
