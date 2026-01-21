# Scheduling

Run the checker periodically to continuously process DLQs.

## Cron

Run every 5 minutes:

```bash
*/5 * * * * /usr/bin/python3 /opt/rmq_retry_checker.py /etc/rmq/config.yaml
```

## Systemd Timer

### Service File

`/etc/systemd/system/rmq-checker.service`:

```ini
[Unit]
Description=RabbitMQ DLQ Retry Checker

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /opt/rmq_retry_checker.py /etc/rmq/config.yaml
```

### Timer File

`/etc/systemd/system/rmq-checker.timer`:

```ini
[Unit]
Description=Run RMQ Retry Checker every 5 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
```

### Enable

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now rmq-checker.timer
```

### Check Status

```bash
systemctl status rmq-checker.timer
systemctl list-timers --all
```
