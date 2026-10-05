package lst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// VerifyIdentity uses the proxy's actual interface: stETH is an Aragon proxy,
// whereas the withdrawal queue is an OssifiableProxy. Pin is only for the
// explicit read-only probe command; normal collection requires stored pins.
func (r *RPC) VerifyIdentity(ctx context.Context, m Manifest, b Block, pin bool) (Manifest, []Response, error) {
	responses := []Response{}
	add := func(res Response) {
		if res.PayloadHash != "" {
			responses = append(responses, res)
		}
	}
	raw, res, e := r.Call(ctx, "eth_chainId", []any{}, "normal")
	add(res)
	if e != nil {
		return m, responses, e
	}
	var chain string
	if json.Unmarshal(raw, &chain) != nil || chain != "0x1" {
		return m, responses, errors.New("wrong_chain")
	}
	if !pin && !m.Pinned() {
		return m, responses, errors.New("manifest_not_pinned_run_probe")
	}
	if m.CodeHashes == nil {
		m.CodeHashes = map[string]string{}
	}
	if m.Pools == nil {
		m.Pools = map[string]string{}
	}
	for _, v := range []struct{ role, sig, impl string }{{"steth", "implementation()", "steth_impl"}, {"queue", "proxy__getImplementation()", "queue_impl"}} {
		out, rs, err := r.View(ctx, m.Address(v.role), b, v.sig, []string{"address"}, "normal")
		add(rs)
		if err != nil {
			return m, responses, err
		}
		if out[0].(common.Address) != common.HexToAddress(m.Addresses[v.impl]) {
			return m, responses, errors.New("unknown_implementation_" + v.role)
		}
	}
	for _, k := range m.CodeRoles() {
		h, rs, err := r.Code(ctx, m.Address(k), b)
		add(rs)
		if err != nil {
			return m, responses, fmt.Errorf("code_%s: %w", k, err)
		}
		if pin {
			m.CodeHashes[k] = Hex(h)
		} else if m.CodeHashes[k] != Hex(h) {
			return m, responses, errors.New("code_hash_mismatch_" + k)
		}
	}
	for _, v := range []struct {
		role string
		dec  uint8
	}{{"steth", 18}, {"wsteth", 18}, {"weth", 18}, {"usdt", 6}} {
		out, rs, err := r.View(ctx, m.Address(v.role), b, "decimals()", []string{"uint8"}, "normal")
		add(rs)
		if err != nil {
			return m, responses, err
		}
		if out[0].(uint8) != v.dec {
			return m, responses, errors.New("wrong_decimals_" + v.role)
		}
	}
	for _, v := range []struct{ role, sig, want string }{{"wsteth", "stETH()", "steth"}, {"queue", "STETH()", "steth"}, {"queue", "WSTETH()", "wsteth"}, {"quoter", "factory()", "factory"}, {"quoter", "WETH9()", "weth"}} {
		out, rs, err := r.View(ctx, m.Address(v.role), b, v.sig, []string{"address"}, "normal")
		add(rs)
		if err != nil {
			return m, responses, err
		}
		if out[0].(common.Address) != common.HexToAddress(m.Addresses[v.want]) {
			return m, responses, errors.New("wrong_identity_" + v.role)
		}
	}
	for i := 0; i < 2; i++ {
		out, rs, err := r.View(ctx, m.Address("curve"), b, "coins(uint256)", []string{"address"}, "normal", big.NewInt(int64(i)))
		add(rs)
		if err != nil {
			return m, responses, err
		}
		want := common.HexToAddress(m.Addresses["steth"])
		if i == 0 {
			want = common.HexToAddress("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE")
		}
		if out[0].(common.Address) != want {
			return m, responses, errors.New("curve_coin_identity")
		}
	}
	for _, v := range []struct {
		name, a, z string
		fee        int64
	}{{"usdt_weth_500", "usdt", "weth", 500}, {"weth_wsteth_100", "weth", "wsteth", 100}} {
		a, z := common.HexToAddress(m.Addresses[v.a]), common.HexToAddress(m.Addresses[v.z])
		out, rs, err := r.View(ctx, m.Address("factory"), b, "getPool(address,address,uint24)", []string{"address"}, "normal", a, z, big.NewInt(v.fee))
		add(rs)
		if err != nil {
			return m, responses, err
		}
		pool := out[0].(common.Address)
		if pool == (common.Address{}) {
			return m, responses, errors.New("pool_missing_" + v.name)
		}
		if pin {
			m.Pools[v.name] = pool.Hex()
		} else if pool != common.HexToAddress(m.Pools[v.name]) {
			return m, responses, errors.New("pool_changed_" + v.name)
		}
		pair := []string{string(a.Bytes()), string(z.Bytes())}
		sort.Strings(pair)
		for i, sig := range []string{"token0()", "token1()"} {
			out, rs, err = r.View(ctx, string(pool.Bytes()), b, sig, []string{"address"}, "normal")
			add(rs)
			if err != nil {
				return m, responses, err
			}
			if string(out[0].(common.Address).Bytes()) != pair[i] {
				return m, responses, errors.New("pool_token_identity")
			}
		}
		out, rs, err = r.View(ctx, string(pool.Bytes()), b, "fee()", []string{"uint24"}, "normal")
		add(rs)
		if err != nil {
			return m, responses, err
		}
		if out[0].(*big.Int).Int64() != v.fee {
			return m, responses, errors.New("pool_fee_identity")
		}
	}
	if r.ProtocolMulticall {
		h, rs, err := r.Code(ctx, multicallAddress(), b)
		add(rs)
		if err != nil {
			return m, responses, fmt.Errorf("multicall_code: %w", err)
		}
		if Hex(h) != multicallCodeHash {
			return m, responses, errors.New("multicall_code_hash_mismatch")
		}
	}

	ok, rs, e := r.Canonical(ctx, b)
	add(rs)
	if e != nil {
		return m, responses, e
	}
	if !ok {
		return m, responses, errors.New("identity_block_orphaned")
	}
	return m, responses, nil
}

func (r *RPC) Protocol(ctx context.Context, m Manifest, b Block) (p ProtocolState, responses []Response, err error) {
	now := Now()
	p = ProtocolState{BlockNumber: &b.Number, BlockHash: &b.Hash, BlockTime: &b.Time, ObservedAt: now, ChainId: 1, QueueAddress: m.Address("queue"), StateStatus: "unknown", AvailableAt: now}
	responses = []Response{b.Response}
	var lastResponse Response
	defer func() {
		p.RequestedAt, p.ReceivedAt, p.SourceAvailableAt, p.PayloadHashes = responseTimes(responses)
		p.AvailableAt = Now()
		if err != nil {
			p.Reason = failureReason("protocol", err, lastResponse)
		}
	}()
	add := func(rs Response) {
		if !r.ProtocolMulticall || rs.PayloadHash != "" || rs.HTTPStatus != 0 {
			lastResponse = rs
		}
		if rs.PayloadHash != "" {
			responses = append(responses, rs)
		}
	}
	var aggregate [][]any
	if r.ProtocolMulticall {
		var rs Response
		aggregate, rs, err = r.protocolMulticall(ctx, m, b)
		add(rs)
		if err != nil {
			return p, responses, err
		}
	}
	index := 0
	view := func(address, sig string, outputs []string, values ...any) ([]any, Response, error) {
		if !r.ProtocolMulticall {
			return r.View(ctx, address, b, sig, outputs, "normal", values...)
		}
		out := aggregate[index]
		index++
		return out, Response{}, nil
	}
	uintView := func(address, sig string, values ...any) (*big.Int, Response, error) {
		out, rs, err := view(address, sig, []string{"uint256"}, values...)
		if err != nil {
			return nil, rs, err
		}
		return out[0].(*big.Int), rs, nil
	}

	for _, v := range []struct{ role, sig, want string }{{"steth", "implementation()", "steth_impl"}, {"queue", "proxy__getImplementation()", "queue_impl"}} {
		out, rs, e := view(m.Address(v.role), v.sig, []string{"address"})
		add(rs)
		if e != nil {
			return p, responses, e
		}
		if out[0].(common.Address) != common.HexToAddress(m.Addresses[v.want]) {
			return p, responses, errors.New("unknown_implementation")
		}
	}
	p.IdentityOk = true
	for _, v := range []struct {
		role, sig string
		dest      **big.Int
	}{{"steth", "getTotalPooledEther()", &p.StethTotalPooledEthWei}, {"steth", "getTotalShares()", &p.StethTotalSharesRaw}, {"queue", "MIN_STETH_WITHDRAWAL_AMOUNT()", &p.MinRequestStethWei}, {"queue", "MAX_STETH_WITHDRAWAL_AMOUNT()", &p.MaxRequestStethWei}, {"queue", "getLastRequestId()", &p.LastRequestId}, {"queue", "getLastFinalizedRequestId()", &p.LastFinalizedRequestId}, {"queue", "unfinalizedStETH()", &p.UnfinalizedStethWei}, {"queue", "getLockedEtherAmount()", &p.LockedEthWei}} {
		n, rs, e := uintView(m.Address(v.role), v.sig)
		add(rs)
		if e != nil {
			return p, responses, e
		}
		*v.dest = n
	}
	n, rs, e := uintView(m.Address("wsteth"), "getStETHByWstETH(uint256)", big.NewInt(1e18))
	add(rs)
	if e != nil {
		return p, responses, e
	}
	p.OneWstethToStethWei = n
	for _, v := range []struct {
		sig  string
		dest **bool
	}{{"isPaused()", &p.QueuePaused}, {"isBunkerModeActive()", &p.BunkerActive}} {
		out, rs, e := view(m.Address("queue"), v.sig, []string{"bool"})
		add(rs)
		if e != nil {
			return p, responses, e
		}
		*v.dest = Ptr(out[0].(bool))
	}
	if p.StethTotalPooledEthWei.Sign() == 0 || p.StethTotalSharesRaw.Sign() == 0 || p.OneWstethToStethWei.Sign() == 0 || p.MaxRequestStethWei.Cmp(p.MinRequestStethWei) < 0 || p.LastFinalizedRequestId.Cmp(p.LastRequestId) > 0 {
		return p, responses, errors.New("protocol_invariant")
	}
	p.BlockNumber = &b.Number
	p.BlockHash = &b.Hash
	p.BlockTime = &b.Time
	p.BaseFeePerGasWei = clone(b.BaseFee)
	p.StateStatus = "ok"
	p.RequestedAt = &responses[0].RequestedAt
	p.ReceivedAt = &responses[len(responses)-1].ReceivedAt
	p.SourceAvailableAt = &responses[len(responses)-1].AvailableAt
	p.AvailableAt = Now()
	for _, rs := range responses {
		p.PayloadHashes = append(p.PayloadHashes, rs.PayloadHash)
	}
	return p, responses, nil
}
func (r *RPC) Swap(ctx context.Context, m Manifest, b Block, in, out string, fee uint32, amount *big.Int) (*big.Int, uint64, Response, error) {
	if !Uint256(amount) || amount.Sign() == 0 {
		return nil, 0, Response{}, errors.New("invalid_swap_amount")
	}
	path := append([]byte(in), byte(fee>>16), byte(fee>>8), byte(fee))
	path = append(path, []byte(out)...)
	v, res, e := r.View(ctx, m.Address("quoter"), b, "quoteExactInput(bytes,uint256)", []string{"uint256", "uint160[]", "uint32[]", "uint256"}, "quoter", path, amount)
	if e != nil {
		return nil, 0, res, e
	}
	n, gas := v[0].(*big.Int), v[3].(*big.Int)
	if !Uint256(n) || n.Sign() == 0 || !gas.IsUint64() {
		return nil, 0, res, errors.New("invalid_quoter_result")
	}
	return n, gas.Uint64(), res, nil
}
func responseTimes(responses []Response) (*time.Time, *time.Time, *time.Time, []string) {
	var first, last *Response
	hashes := []string{}
	for i := range responses {
		r := &responses[i]
		if r.PayloadHash == "" {
			continue
		}
		hashes = append(hashes, r.PayloadHash)
		if first == nil || r.RequestedAt.Before(first.RequestedAt) {
			first = r
		}
		if last == nil || r.AvailableAt.After(last.AvailableAt) {
			last = r
		}
	}
	if first == nil {
		return nil, nil, nil, hashes
	}
	return Ptr(first.RequestedAt), Ptr(last.ReceivedAt), Ptr(last.AvailableAt), hashes
}
