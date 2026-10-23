package mysql

import (
	"context"
	"fmt"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

func (c *Client) readKeeperRentalState(ctx context.Context, q executor, b *keeper.Batch) error {
	available, err := c.keeperRentalStateAvailable(ctx, q)
	if err != nil {
		return err
	}
	if !available {
		if b.Capture.CaptureKind == "rental_state" {
			return fmt.Errorf("keeper rental-state schema requires administrative installation")
		}
		return nil
	}
	b.RentalStates, err = readModels[keeper.RentalStateObservation](ctx, c, q, "jl_keeper_rental_state_observation", "WHERE capture_id=? ORDER BY observation_index", b.Capture.CaptureId[:])
	if err != nil {
		return err
	}
	proofs, err := readModels[keeper.RentalStateBatchProof](ctx, c, q, "jl_keeper_rental_state_batch", "WHERE capture_id=?", b.Capture.CaptureId[:])
	if err != nil {
		return err
	}
	if len(proofs) > 1 {
		return fmt.Errorf("duplicate keeper rental-state proof")
	}
	if len(proofs) == 1 {
		b.RentalStateProof = &proofs[0]
	}
	return nil
}

func (c *Client) writeKeeperRentalState(ctx context.Context, q executor, b keeper.Batch) error {
	available, err := c.keeperRentalStateAvailable(ctx, q)
	if err != nil {
		return err
	}
	if !available {
		if b.Capture.CaptureKind == "rental_state" {
			return fmt.Errorf("keeper rental-state schema requires administrative installation")
		}
		return nil
	}
	if err = putModels(ctx, c, q, "jl_keeper_rental_state_observation", b.RentalStates); err != nil {
		return err
	}
	if b.RentalStateProof != nil {
		return putModels(ctx, c, q, "jl_keeper_rental_state_batch", []keeper.RentalStateBatchProof{*b.RentalStateProof})
	}
	return nil
}
