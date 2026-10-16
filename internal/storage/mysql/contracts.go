package mysql

import "github.com/vphoenix/crypto-market-info/internal/storage/contracts"

var _ contracts.MainStore = (*Client)(nil)
var _ contracts.OptionsStore = (*Client)(nil)
var _ contracts.AcrossStore = (*Client)(nil)
var _ contracts.ReserveStore = (*Client)(nil)
var _ contracts.LSTStore = (*Client)(nil)
var _ contracts.KeeperStore = (*Client)(nil)
var _ contracts.DEXStore = (*Client)(nil)
var _ contracts.PerpetualReader = (*Client)(nil)
