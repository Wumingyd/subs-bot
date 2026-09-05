#!/usr/bin/env bash
set -euo pipefail

# subs-bot one-command deploy
# Usage:  ./deploy.sh
# Prereq: Python 3.11+, systemd, git

INSTALL_DIR="${INSTALL_DIR:-/opt/subs-bot}"
VENV_DIR="${INSTALL_DIR}/.venv"

echo "=== subs-bot deploy ==="
echo "  install dir: ${INSTALL_DIR}"

if [ -d "${INSTALL_DIR}/.git" ]; then
    echo ">>> git pull..."
    cd "${INSTALL_DIR}" && git pull --ff-only
else
    REPO_URL="${REPO_URL:-$(git config --get remote.origin.url || echo '')}"
    if [ -z "${REPO_URL}" ]; then
        echo "ERROR: clone this repo first or set REPO_URL env var"
        exit 1
    fi
    echo ">>> git clone ${REPO_URL} -> ${INSTALL_DIR}"
    git clone "${REPO_URL}" "${INSTALL_DIR}"
fi

cd "${INSTALL_DIR}"

echo ">>> creating venv..."
python3 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/pip" install --upgrade pip -q
"${VENV_DIR}/bin/pip" install -r requirements.txt -q

if [ ! -f "${INSTALL_DIR}/.env" ]; then
    echo ">>> copying .env.example -> .env"
    cp .env.example .env
    echo "  !! Edit .env: set BOT_TOKEN, ALLOWED_USER_IDS, PUBLIC_BASE_URL"
    echo "  !! Then re-run: ./deploy.sh"
    exit 0
fi

echo ">>> installing systemd service..."
cat > /etc/systemd/system/subs-bot.service <<UNIT
[Unit]
Description=Telegram Subs Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${INSTALL_DIR}/.env
ExecStart=${VENV_DIR}/bin/python ${INSTALL_DIR}/bot.py
Restart=always
RestartSec=3
User=root

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable subs-bot
systemctl restart subs-bot

sleep 2
echo ">>> status:"
systemctl is-active subs-bot
echo "=== done ==="
echo "  service:  systemctl status subs-bot"
echo "  logs:     journalctl -u subs-bot -f"
echo "  health:   curl http://localhost:8787/health"
