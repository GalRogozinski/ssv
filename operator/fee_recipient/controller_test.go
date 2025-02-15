package fee_recipient

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/bloxapp/eth2-key-manager/core"
	spectypes "github.com/bloxapp/ssv-spec/types"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/async/event"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/bloxapp/ssv/operator/slot_ticker/mocks"
	"github.com/bloxapp/ssv/operator/validator"
	"github.com/bloxapp/ssv/protocol/v2/blockchain/beacon"
	"github.com/bloxapp/ssv/protocol/v2/types"
	"github.com/bloxapp/ssv/storage"
	"github.com/bloxapp/ssv/storage/basedb"
	"github.com/bloxapp/ssv/utils/logex"
)

func init() {
	logex.Build("", zapcore.DebugLevel, &logex.EncodingConfig{})
}

func TestSubmitProposal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	operatorKey := "123456789"

	db, collection := createStorage(t)
	defer db.Close()
	network := beacon.NewNetwork(core.PraterNetwork, 0)
	populateStorage(t, collection, operatorKey)

	frCtrl := NewController(&ControllerOptions{
		Logger:            logex.GetLogger(),
		Ctx:               context.TODO(),
		EthNetwork:        network,
		ShareStorage:      collection,
		OperatorPublicKey: operatorKey,
	})

	t.Run("submit first time or first slot in epoch", func(t *testing.T) {
		numberOfRequests := 4
		var wg sync.WaitGroup
		client := beacon.NewMockBeacon(ctrl)
		client.EXPECT().SubmitProposalPreparation(gomock.Any()).DoAndReturn(func(feeRecipients map[phase0.ValidatorIndex]bellatrix.ExecutionAddress) error {
			wg.Done()
			return nil
		}).MinTimes(numberOfRequests).MaxTimes(numberOfRequests) // call first time and on the first slot of epoch. each time should be 2 request as we have two batches

		ticker := mocks.NewMockTicker(ctrl)
		ticker.EXPECT().Subscribe(gomock.Any()).DoAndReturn(func(subscription chan phase0.Slot) event.Subscription {
			subscription <- 1 // first time
			time.Sleep(time.Millisecond * 500)
			subscription <- 2 // should not call submit
			time.Sleep(time.Millisecond * 500)
			subscription <- 20 // should not call submit
			time.Sleep(time.Millisecond * 500)
			subscription <- 32 // first slot of epoch
			time.Sleep(time.Millisecond * 500)
			subscription <- 63 // should not call submit
			return nil
		})

		frCtrl.beaconClient = client
		frCtrl.ticker = ticker

		go frCtrl.Start()
		wg.Add(numberOfRequests)
		wg.Wait()
	})

	t.Run("error handling", func(t *testing.T) {
		var wg sync.WaitGroup
		client := beacon.NewMockBeacon(ctrl)
		client.EXPECT().SubmitProposalPreparation(gomock.Any()).DoAndReturn(func(feeRecipients map[phase0.ValidatorIndex]bellatrix.ExecutionAddress) error {
			wg.Done()
			return errors.New("failed to submit")
		}).MinTimes(2).MaxTimes(2)

		ticker := mocks.NewMockTicker(ctrl)
		ticker.EXPECT().Subscribe(gomock.Any()).DoAndReturn(func(subscription chan phase0.Slot) event.Subscription {
			subscription <- 100 // first time
			return nil
		})

		frCtrl.beaconClient = client
		frCtrl.ticker = ticker

		go frCtrl.Start()
		wg.Add(2)
		wg.Wait()
	})
}

func createStorage(t *testing.T) (basedb.IDb, validator.ICollection) {
	options := basedb.Options{
		Type:   "badger-memory",
		Logger: logex.GetLogger(),
		Path:   "",
	}

	db, err := storage.GetStorageFactory(options)
	require.NoError(t, err)

	return db, validator.NewCollection(validator.CollectionOptions{
		DB:     db,
		Logger: options.Logger,
	})
}

func populateStorage(t *testing.T, storage validator.ICollection, operatorKey string) {
	createShare := func(index int, operatorKey string) *types.SSVShare {
		ownerAddr := fmt.Sprintf("%d", index)
		ownerAddrByte := [20]byte{}
		copy(ownerAddrByte[:], ownerAddr)

		return &types.SSVShare{
			Share: spectypes.Share{ValidatorPubKey: []byte(fmt.Sprintf("pk%d", index))},
			Metadata: types.Metadata{
				BeaconMetadata: &beacon.ValidatorMetadata{
					Index: phase0.ValidatorIndex(index),
				},
				OwnerAddress: hex.EncodeToString(ownerAddrByte[:]),
				Operators:    [][]byte{[]byte(operatorKey)},
				Liquidated:   false,
			},
		}
	}

	for i := 0; i < 1000; i++ {
		require.NoError(t, storage.SaveValidatorShare(createShare(i, operatorKey)))
	}

	// add none committee share
	require.NoError(t, storage.SaveValidatorShare(createShare(2000, "operatorpubkey")))

	all, err := storage.GetFilteredValidatorShares(validator.NotLiquidatedAndByOperatorPubKey(operatorKey))
	require.NoError(t, err)
	require.Equal(t, 1000, len(all))

}
