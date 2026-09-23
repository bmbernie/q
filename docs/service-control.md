# Service Control

Status:

    systemctl --user status qd.service

Restart:

    systemctl --user restart qd.service

Stop:

    systemctl --user stop qd.service

Start:

    systemctl --user start qd.service

Service logs:

    journalctl --user -u qd.service
