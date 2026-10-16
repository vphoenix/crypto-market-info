package mysql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/across"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"time"
)

// These exact-key reads are used only before a NEW stopped-service import plan.
// Absence is not commit resolution: admitted unknown operations are retried by
// the original transaction and never use these helpers to declare completion.
func (c *Client) AcrossCaptureExists(ctx context.Context, cap across.Capture) (bool, error) {
	_, err := c.AcrossCapture(ctx, cap)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
func (c *Client) ReserveCaptureExists(ctx context.Context, cap reserve.Capture) (bool, error) {
	_, err := c.ReserveCapture(ctx, cap)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
func (c *Client) LSTCaptureExists(ctx context.Context, cap lst.Capture) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, ok, err := c.lstCapture(ctx, cap.CaptureId)
	return ok, err
}
func (c *Client) KeeperCaptureExists(ctx context.Context, cap keeper.Capture) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var one int
	err := c.db.QueryRowContext(ctx, "SELECT 1 FROM "+c.table("jl_keeper_capture")+" WHERE capture_id=?", cap.CaptureId[:]).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
