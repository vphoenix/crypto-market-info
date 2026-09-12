# 桌面采集状态指示器

这是一个只读的 Ubuntu 顶栏 AppIndicator。默认每 30 秒检查一次：

- 生产 collector 与全量永续验收 unit 的 `ActiveState`；
- ClickHouse 最近十分钟内的五条生产盘口流；
- 最近八小时内的收益观测写入时间。

它不写数据库、不访问交易所接口，也不持有任何凭据。绿色要求生产 collector
运行、五条盘口流都有最新完整的 `60/60` 分钟，并且最近收益写入不超过两小时。
黄色表示部分降级，红色表示生产 collector、ClickHouse 或盘口更新已经中断。

先执行一次无桌面依赖的检查：

```bash
python3 tools/desktop-status/crypto_market_status.py --once
```

Ubuntu 24.04 需要 AppIndicator 的 Python GI 绑定：

```bash
sudo apt-get install gir1.2-ayatanaappindicator3-0.1
```

安装并启动用户级图形会话服务：

```bash
install -Dm644 deploy/systemd/crypto-market-info-status-indicator.service \
  /home/ubuntu/.config/systemd/user/crypto-market-info-status-indicator.service
systemctl --user daemon-reload
systemctl --user enable --now crypto-market-info-status-indicator.service
```

查看资源占用和日志：

```bash
systemctl --user status crypto-market-info-status-indicator.service
journalctl --user -u crypto-market-info-status-indicator.service -n 80 --no-pager
```
