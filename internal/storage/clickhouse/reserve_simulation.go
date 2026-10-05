package clickhouse

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"reflect"
)

func (c *Client) WriteReserveSimulation(ctx context.Context, s reserve.Simulation) error {
	if e := reserve.ValidateSimulation(s); e != nil {
		return e
	}
	return c.dexInsert(ctx, "reserve_route_simulation", reserveColumns(reflect.TypeOf(s)), reserveRows([]reserve.Simulation{s}))
}
func (c *Client) ReserveSimulations(ctx context.Context, manifest string) ([]reserve.Simulation, error) {
	rows, e := reserveRead[reserve.Simulation](ctx, c, "reserve_route_simulation", "WHERE chain_id=1 AND manifest_hash=? ORDER BY block_number,quote_id,available_at", manifest)
	if e != nil {
		return nil, e
	}
	for _, s := range rows {
		if e = reserve.ValidateSimulation(s); e != nil {
			return nil, e
		}
	}
	return rows, nil
}
