package topics

import (
	"context"
	"github.com/bloxapp/ssv-spec/qbft"
	spectypes "github.com/bloxapp/ssv-spec/types"
	"github.com/bloxapp/ssv/operator/validator"
	"github.com/bloxapp/ssv/protocol/v2/qbft/controller"
	"github.com/bloxapp/ssv/protocol/v2/types"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"

	"github.com/bloxapp/ssv/network/forks"
)

// MsgValidatorFunc represents a message validator
type MsgValidatorFunc = func(ctx context.Context, p peer.ID, msg *pubsub.Message) pubsub.ValidationResult

// NewSSVMsgValidator creates a new msg validator that validates message structure,
// and checks that the message was sent on the right topic.
// TODO - copying plogger may cause GC issues, consider using a pool
// TODO: enable post SSZ change, remove logs, break into smaller validators?
func NewSSVMsgValidator(plogger zap.Logger, fork forks.Fork, valController validator.Controller) func(ctx context.Context, p peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	schedule := NewMessageSchedule()
	return func(ctx context.Context, p peer.ID, pmsg *pubsub.Message) pubsub.ValidationResult {
		plog := plogger.With(zap.String("peerID", pmsg.GetFrom().String()))
		topic := pmsg.GetTopic()
		metricPubsubActiveMsgValidation.WithLabelValues(topic).Inc()
		defer metricPubsubActiveMsgValidation.WithLabelValues(topic).Dec()
		if len(pmsg.GetData()) == 0 {
			reportValidationResult(validationResultNoData, plog, nil, "")
			return pubsub.ValidationReject
		}
		msg, err := fork.DecodeNetworkMsg(pmsg.GetData())
		if err != nil {
			// can't decode message
			// logger.Debug("invalid: can't decode message", zap.Error(err))
			reportValidationResult(validationResultEncoding, plog, err, "")
			return pubsub.ValidationReject
		}
		if msg == nil {
			reportValidationResult(validationResultEncoding, plog, nil, "")
			return pubsub.ValidationReject
		}
		pmsg.ValidatorData = *msg

		//if valFunc == nil {
		//	plogger.Warn("no message validator provided", zap.String("topic", topic))
		//	return pubsub.ValidationAccept
		//}

		return ValidateSSVMsg(ctx, msg, valController, schedule, plog)

		// Check if the message was sent on the right topic.
		// currentTopic := pmsg.GetTopic()
		// currentTopicBaseName := fork.GetTopicBaseName(currentTopic)
		// topics := fork.ValidatorTopicID(msg.GetID().GetPubKey())
		// for _, tp := range topics {
		//	if tp == currentTopicBaseName {
		//		reportValidationResult(validationResultValid)
		//		return pubsub.ValidationAccept
		//	}
		//}
		// reportValidationResult(validationResultTopic)
		// return pubsub.ValidationReject
	}
}

func ValidateSSVMsg(ctx context.Context, msg *spectypes.SSVMessage, valController validator.Controller, schedule *MessageSchedule, plogger *zap.Logger) pubsub.ValidationResult {
	switch msg.MsgType {
	case spectypes.SSVConsensusMsgType:
		share, err := valController.GetShare(msg.MsgID.GetPubKey())
		if err != nil || share == nil {
			reportValidationResult(validationResultNoValidator, plogger, err, "")
			return pubsub.ValidationReject
		}
		signedMsg := qbft.SignedMessage{}
		err = signedMsg.Decode(msg.GetData())
		if err != nil {
			reportValidationResult(validationResultEncoding, plogger, err, "")
			return pubsub.ValidationReject
		}
		return validateConsesnsusMsg(ctx, &signedMsg, share, schedule, plogger)
	default:
		return pubsub.ValidationAccept
	}
}

/*
Main controller processing flow

All decided msgs are processed the same, out of instance
All valid future msgs are saved in a container and can trigger the highest decided future msg
All other msgs (not future or decided) are processed normally by an existing instance (if found)
*/
func validateConsesnsusMsg(ctx context.Context, signedMsg *qbft.SignedMessage, share *types.SSVShare, schedule *MessageSchedule, plogger *zap.Logger) pubsub.ValidationResult {
	if signedMsg.Validate() != nil {
		reportValidationResult(ValidationResultSyntacticCheck, plogger, nil, "")
		return pubsub.ValidationReject
	}

	// TODO can this be inside another syntactic check? maybe change decided.HasQuroum?
	if len(signedMsg.Signers) > len(share.Committee) {
		reportValidationResult(ValidationResultSyntacticCheck, plogger, nil, "too many signers")
		return pubsub.ValidationReject
	}

	// if isDecided msg (this propagates to all topics)
	if controller.IsDecidedMsg(&share.Share, signedMsg) {
		return validateDecideMessage(signedMsg, schedule, plogger, share)
	}

	//if non-decided msg and I am a committee-validator
	//is message is timely?
	//if no instance of qbft
	// base validation
	// sig validation
	// mark message

	// if qbft instance is decided (I am a validator)
	// base commit message validation
	// sig commit message validation
	// is commit msg aggratable
	// mark message

	// If qbft instance is not decided (I am a validator)
	// Full validation of messages
	//mark consensus message

	return validateUnDecidedMsg(schedule, signedMsg, plogger, share)
}

func validateUnDecidedMsg(schedule *MessageSchedule, signedMsg *qbft.SignedMessage, plogger *zap.Logger, share *types.SSVShare) pubsub.ValidationResult {
	plogger = plogger.With(zap.String("msgType", string(signedMsg.Message.MsgType)))
	// If I am a non-committee-validator (or I don't have access to a qbft instance)
	// is message timely
	if !schedule.isConsensusMsgTimely(signedMsg, plogger) {
		reportValidationResult(ValidationResultNotTimely, plogger, nil, "")
		return pubsub.ValidationReject
	}

	//err := signedMsg.Validate()
	//if err != nil {
	//	reportValidationResult(ValidationResultSyntacticCheck, plogger, err, "")
	//	return pubsub.ValidationReject
	//}

	//propose case
	// propose base validation
	// is propose message timely
	//

	// sig validation
	if signedMsg.Signature.VerifyByOperators(signedMsg, share.DomainType, spectypes.QBFTSignatureType, share.Committee) != nil {
		reportValidationResult(ValidationResultInvalidSig, plogger, nil, "message type is "+string(signedMsg.Message.MsgType))
		return pubsub.ValidationReject
	}

	// mark message
	schedule.MarkConsensusMessage(signedMsg.Message.Identifier, signedMsg.Signers[0], signedMsg.Message.Round, signedMsg.Message.MsgType)

	// base commit message validation
	// sig commit message validation
	// is commit msg aggratable
	// mark message

	return pubsub.ValidationAccept
}

func validateDecideMessage(signedCommit *qbft.SignedMessage, schedule *MessageSchedule, plogger *zap.Logger, share *types.SSVShare) pubsub.ValidationResult {
	plogger = plogger.
		With(zap.String("msg_type", "decided")).
		With(zap.Any("signers", signedCommit.Signers)).
		With(zap.Any("height", signedCommit.Message.Height))

	// check if better decided
	// Mainly to have better statistics on commitments with a full signer set
	// TODO can it cause liveness failure?
	if schedule.hasBetterMsg(signedCommit) {
		reportValidationResult(ValidationResultBetterMessage, plogger, nil, string(signedCommit.Message.Identifier))
		return pubsub.ValidationIgnore
	}

	//check if timely decided
	if !schedule.isTimelyDecidedMsg(signedCommit, plogger) {
		reportValidationResult(ValidationResultNotTimely, plogger, nil, "")
		return pubsub.ValidationReject
	}

	// validate decided message
	// TODO calls signedMsg.validate() again
	// TODO should do deeper syntax validation for commit data? Or eth protocol protects well enough?
	if err := controller.ValidateDecidedSyntactically(signedCommit, &share.Share); err != nil {
		reportValidationResult(ValidationResultSyntacticCheck, plogger, err, "")
		return pubsub.ValidationReject
	}
	// verify signature
	///TODO: option 1, offload signature verification to this layer. option 2, do better aggregation
	if err := signedCommit.Signature.VerifyByOperators(signedCommit, share.DomainType, spectypes.QBFTSignatureType, share.Committee); err != nil {
		reportValidationResult(ValidationResultInvalidSig, plogger, err, "")
		return pubsub.ValidationReject
	}

	//mark decided message
	schedule.markDecidedMsg(signedCommit, plogger)
	return pubsub.ValidationAccept
}
