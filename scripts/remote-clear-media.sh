#!/usr/bin/env bash
set -euo pipefail

# ══════════════════════════════════════════════════════════
#  EDIT THESE VARIABLES BEFORE RUNNING
# ══════════════════════════════════════════════════════════
REMOTE_IP="34.126.219.33"
REMOTE_USER="sagardeka"
SSH_PRIVATE_KEY="$HOME/.ssh/id_rsa"
SSH_PASSPHRASE="I am the best"
# ══════════════════════════════════════════════════════════

DOWNLOADS_DIR="/home/${REMOTE_USER}/yt-downloads"
DB_PATH="${DOWNLOADS_DIR}/data/ytgrab.db"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

if [[ ! -f "$SSH_PRIVATE_KEY" ]]; then
  echo "Error: SSH private key not found at '$SSH_PRIVATE_KEY'" >&2
  exit 1
fi

eval "$(ssh-agent -s)" > /dev/null
cleanup() { ssh-agent -k > /dev/null 2>&1 || true; }
trap cleanup EXIT

export SSH_ASKPASS_REQUIRE=force
export SSH_ASKPASS="$(mktemp)"
chmod +x "$SSH_ASKPASS"
echo "#!/bin/sh" > "$SSH_ASKPASS"
echo "echo '${SSH_PASSPHRASE}'" >> "$SSH_ASKPASS"
ssh-add "$SSH_PRIVATE_KEY" 2>/dev/null
rm -f "$SSH_ASKPASS"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Target     : ${REMOTE_USER}@${REMOTE_IP}"
echo "  DB         : ${DB_PATH}"
echo "  Media dirs : ${DOWNLOADS_DIR}/downloads/{audio,video}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_IP}" "sudo bash -lc '
set -euo pipefail

DB_PATH=\"${DB_PATH}\"
AUDIO_DIR=\"${DOWNLOADS_DIR}/downloads/audio\"
VIDEO_DIR=\"${DOWNLOADS_DIR}/downloads/video\"

count_files() {
  local d=\"\$1\"
  if [[ -d \"\$d\" ]]; then
    find \"\$d\" -type f | wc -l
  else
    echo 0
  fi
}

AUDIO_BEFORE=\$(count_files \"\$AUDIO_DIR\")
VIDEO_BEFORE=\$(count_files \"\$VIDEO_DIR\")

python3 - <<PY
import sqlite3
db_path = \"${DB_PATH}\"
conn = sqlite3.connect(db_path)
cur = conn.cursor()
media_before = cur.execute(\"SELECT count(1) FROM media\").fetchone()[0]
cache_sources_before = cur.execute(\"SELECT count(1) FROM preview_sources\").fetchone()[0]
cache_items_before = cur.execute(\"SELECT count(1) FROM preview_items\").fetchone()[0]

cur.execute(\"DELETE FROM media WHERE mode IN ('audio','video')\")
cur.execute(\"DELETE FROM transcripts\")
conn.commit()

media_after = cur.execute(\"SELECT count(1) FROM media\").fetchone()[0]
cache_sources_after = cur.execute(\"SELECT count(1) FROM preview_sources\").fetchone()[0]
cache_items_after = cur.execute(\"SELECT count(1) FROM preview_items\").fetchone()[0]

print(
    f\"db_media_before={media_before} db_media_after={media_after} \"
    f\"cache_sources_before={cache_sources_before} cache_sources_after={cache_sources_after} \"
    f\"cache_items_before={cache_items_before} cache_items_after={cache_items_after}\"
)
PY

mkdir -p \"\$AUDIO_DIR\" \"\$VIDEO_DIR\"
find \"\$AUDIO_DIR\" -mindepth 1 -delete
find \"\$VIDEO_DIR\" -mindepth 1 -delete

AUDIO_AFTER=\$(count_files \"\$AUDIO_DIR\")
VIDEO_AFTER=\$(count_files \"\$VIDEO_DIR\")

echo \"audio_files_before=\$AUDIO_BEFORE audio_files_after=\$AUDIO_AFTER\"
echo \"video_files_before=\$VIDEO_BEFORE video_files_after=\$VIDEO_AFTER\"
'"

echo "✔ Done. Media DB rows and audio/video files cleared. Preview cache kept."
