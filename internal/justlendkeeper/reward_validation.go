package justlendkeeper

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"time"
)

func probeIdentityVerified(p Probe) bool {
	implementation, _ := HexAddress(ImplementationHex)
	return p.IdentityStatus == "verified_manifest" && p.ImplementationAddress != nil && *p.ImplementationAddress == implementation &&
		p.ImplementationCodeHash != nil && *p.ImplementationCodeHash == mustHex(ImplementationSHA) && p.ImplementationCheckedAt != nil &&
		!p.ImplementationCheckedAt.IsZero() && p.RequestStartedAt != nil && !p.ImplementationCheckedAt.After(*p.RequestStartedAt) &&
		p.AvailableAt.Sub(*p.ImplementationCheckedAt) <= 10*time.Minute
}

func probeTVMSucceeded(value string) bool {
	return value == "SUCESS_default" || value == `"SUCESS"` || value == "0"
}

// The parser can classify before the trailing head arrives. Storage must call
// this after finalization: no positive observation may commit without provenance.
func validatePositiveProbe(p Probe) error {
	if p.Status != "success_reward" {
		return nil
	}
	if !probeIdentityVerified(p) || p.ApiSuccess == nil || !*p.ApiSuccess || !probeTVMSucceeded(p.TvmResult) ||
		p.ErrorCode != "" || p.ErrorMessage != "" || p.RewardConsistency != "matched" {
		return errors.New("positive_probe_execution_unverified")
	}
	for _, v := range []*big.Int{p.RewardReturnSun, p.RewardLogSun, p.RewardTransferSun} {
		if v == nil || v.Sign() <= 0 || v.BitLen() > 256 {
			return errors.New("positive_probe_reward_invalid")
		}
	}
	if p.RewardReturnSun.Cmp(p.RewardLogSun) != 0 || p.RewardReturnSun.Cmp(p.RewardTransferSun) != 0 {
		return errors.New("positive_probe_reward_mismatch")
	}
	contract, _ := HexAddress(ContractHex)
	if p.ContractAddress != contract || p.ResourceType != 1 || p.EndpointView != "wallet_latest" || p.StateBinding != "node_latest_unpinned" {
		return errors.New("positive_probe_identity_invalid")
	}
	for _, a := range []string{p.Renter, p.Receiver, p.CallerAddress} {
		if len(a) != 21 || a[0] != 0x41 {
			return errors.New("positive_probe_address_invalid")
		}
	}
	if p.CallerAddress == p.Renter || p.CallerAddress == p.Receiver {
		return errors.New("positive_probe_caller_conflict")
	}
	if p.RequestPayloadHash == nil || len(*p.RequestPayloadHash) != 32 || p.ResponsePayloadHash == nil || len(*p.ResponsePayloadHash) != 32 ||
		p.RequestStartedAt == nil || p.RequestStartedAt.IsZero() || p.ResponseReceivedAt == nil || p.ResponseReceivedAt.IsZero() || p.AvailableAt.IsZero() ||
		p.ResponseReceivedAt.Before(*p.RequestStartedAt) || p.AvailableAt.Before(*p.ResponseReceivedAt) {
		return errors.New("positive_probe_evidence_missing")
	}
	if p.HeadBeforeNumber == nil || p.HeadAfterNumber == nil || *p.HeadBeforeNumber == 0 || *p.HeadAfterNumber < *p.HeadBeforeNumber ||
		p.HeadBeforeHash == nil || len(*p.HeadBeforeHash) != 32 || p.HeadAfterHash == nil || len(*p.HeadAfterHash) != 32 ||
		p.HeadBeforeTime == nil || p.HeadBeforeTime.IsZero() || p.HeadAfterTime == nil || p.HeadAfterTime.Before(*p.HeadBeforeTime) {
		return errors.New("positive_probe_heads_missing")
	}
	if *p.HeadBeforeNumber == *p.HeadAfterNumber && (*p.HeadBeforeHash != *p.HeadAfterHash || !p.HeadBeforeTime.Equal(*p.HeadAfterTime)) {
		return errors.New("positive_probe_heads_conflict")
	}
	return nil
}

func applyProbeRewardEvidence(p *Probe, logs []Log, internal json.RawMessage) error {
	reward := big.NewInt(0)
	count := 0
	conflict := false
	for _, l := range logs {
		address, err := Address(l.Address)
		if err != nil || address != p.ContractAddress || len(l.Topics) == 0 {
			continue
		}
		topic, err := BinaryHex(l.Topics[0], 32)
		if err != nil || topic != Topic(signatures["Liquidate"]) {
			continue
		}
		row, err := DecodeLog(l)
		if err != nil {
			conflict = true
			continue
		}
		if row.Renter != p.Renter || row.Receiver != p.Receiver || row.Liquidator == nil || *row.Liquidator != p.CallerAddress {
			conflict = true
			continue
		}
		reward.Add(reward, row.RewardSun)
		count++
	}
	if count > 0 {
		p.RewardLogSun = reward
	}
	if count > 1 {
		conflict = true
	}
	transfer, err := probeNativeReward(internal, p.ContractAddress, p.CallerAddress)
	if err != nil {
		return err
	}
	p.RewardTransferSun = transfer
	if conflict {
		p.RewardConsistency = "mismatch"
		return nil
	}
	if p.RewardLogSun != nil && p.RewardTransferSun != nil {
		p.RewardConsistency = "mismatch"
		if p.RewardReturnSun.Cmp(p.RewardLogSun) == 0 && p.RewardReturnSun.Cmp(p.RewardTransferSun) == 0 {
			p.RewardConsistency = "matched"
		}
	}
	return nil
}

// Only this new simulation path filters action notes. Legacy Transfers and its
// stored NativeTransfer encoding intentionally remain unchanged.
func probeNativeReward(raw json.RawMessage, contract, caller string) (*big.Int, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var rows []struct {
		From     string                       `json:"caller_address"`
		To       string                       `json:"transferTo_address"`
		Rejected json.RawMessage              `json:"rejected"`
		Values   []map[string]json.RawMessage `json:"callValueInfo"`
		Note     string                       `json:"note"`
	}
	if err := Decode(raw, &rows); err != nil {
		return nil, err
	}
	sum := big.NewInt(0)
	for _, r := range rows {
		from, err := Address(r.From)
		if err != nil {
			return nil, err
		}
		to, err := Address(r.To)
		if err != nil {
			return nil, err
		}
		if from != contract || to != caller {
			continue
		}
		if len(r.Rejected) > 0 {
			if bytes.Equal(bytes.TrimSpace(r.Rejected), []byte("null")) {
				return nil, nil
			}
			var rejected bool
			if err = json.Unmarshal(r.Rejected, &rejected); err != nil {
				return nil, err
			}
			if rejected {
				continue
			}
		}
		note, err := hex.DecodeString(r.Note)
		if err != nil {
			return nil, nil
		}
		switch string(note) {
		case "call", "create", "suicide":
		case "unDelegateResourceOfEnergy", "unDelegateResourceOfBandwidth", "delegateResourceOfEnergy", "delegateResourceOfBandwidth", "freezeBalanceV2", "unfreezeBalanceV2", "withdrawExpireUnfreeze", "cancelAllUnfreezeV2":
			continue
		default:
			return nil, nil
		}
		if r.Values == nil {
			return nil, nil
		}
		for _, v := range r.Values {
			if v == nil {
				return nil, nil
			}
			if token, ok := v["tokenId"]; ok {
				var id string
				if err = json.Unmarshal(token, &id); err != nil {
					return nil, err
				}
				if bytes.Equal(bytes.TrimSpace(token), []byte("null")) {
					return nil, nil
				}
				if id != "" {
					continue
				}
			}
			amount := big.NewInt(0)
			if value, ok := v["callValue"]; ok {
				amount, err = Uint(value)
				if err != nil {
					return nil, err
				}
			}
			sum.Add(sum, amount)
			if sum.BitLen() > 256 {
				return nil, errors.New("reward_transfer_uint256_overflow")
			}
		}
	}
	return sum, nil
}
