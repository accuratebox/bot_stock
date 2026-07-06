#!/usr/bin/env bash
set -euo pipefail

# NordVPN login + connect helper (Dominican Republic)
# Usage:
#   NORDVPN_EMAIL='you@example.com' NORDVPN_PASSWORD='***' ./scripts/nordvpn_connect_do.sh
# or:
#   ./scripts/nordvpn_connect_do.sh   (will prompt securely)
#   ./scripts/nordvpn_connect_do.sh --non-interactive

REQUIRED_COUNTRY="${REQUIRED_VPN_COUNTRY:-Dominican Republic}"
NON_INTERACTIVE=0

if [[ "${1:-}" == "--non-interactive" ]]; then
  NON_INTERACTIVE=1
fi

if ! command -v nordvpn >/dev/null 2>&1; then
  echo "Error: nordvpn CLI no esta instalado o no esta en PATH." >&2
  exit 1
fi

email="${NORDVPN_EMAIL:-}"
password="${NORDVPN_PASSWORD:-}"
token="${NORDVPN_TOKEN:-}"

if [[ -z "$email" && $NON_INTERACTIVE -eq 0 ]]; then
  read -r -p "NordVPN email: " email
fi

if [[ -z "$password" && $NON_INTERACTIVE -eq 0 ]]; then
  read -r -s -p "NordVPN password: " password
  echo
fi

# Login priority for automation:
# 1) token login (supported on modern NordVPN CLI)
# 2) username/password login (legacy CLI only)
if [[ -n "$token" ]]; then
  nordvpn login --token "$token" || true
elif [[ -n "$email" && -n "$password" ]]; then
  if nordvpn login --help 2>/dev/null | grep -qi -- '--username'; then
    nordvpn login --username "$email" --password "$password" || true
  else
    if [[ $NON_INTERACTIVE -eq 0 ]]; then
      echo "Tu version de NordVPN no soporta login por username/password en CLI." >&2
      echo "Usa: NORDVPN_TOKEN en .env o ejecuta 'nordvpn login' una vez (flujo web)." >&2
    fi
  fi
fi

nordvpn set autoconnect on "${REQUIRED_COUNTRY// /_}" >/dev/null 2>&1 || true

connect_ok=0
for candidate in "$REQUIRED_COUNTRY" "${REQUIRED_COUNTRY// /_}" "Dominican_Republic" "Dominican Republic" "do"; do
  if nordvpn connect "$candidate" >/dev/null 2>&1; then
    connect_ok=1
    break
  fi
done

if [[ $connect_ok -ne 1 ]]; then
  echo "Error: no se pudo conectar NordVPN a '$REQUIRED_COUNTRY'." >&2
  exit 6
fi

status="$(nordvpn status 2>/dev/null || true)"
country="$(printf '%s\n' "$status" | awk -F': ' 'tolower($1)=="country" {print $2; exit}')"
state="$(printf '%s\n' "$status" | awk -F': ' 'tolower($1)=="status" {print $2; exit}')"

if [[ "${state,,}" != *"connected"* ]]; then
  echo "Error: VPN no conectada." >&2
  exit 2
fi

country_norm="${country,,}"
country_norm="${country_norm//_/ }"
required_norm="${REQUIRED_COUNTRY,,}"
required_norm="${required_norm//_/ }"

if [[ "$country_norm" != *"$required_norm"* && "$country_norm" != *"dominican republic"* ]]; then
  echo "Error: VPN conectada, pero pais actual es '$country' y se requiere '$REQUIRED_COUNTRY'." >&2
  exit 3
fi

echo "OK: NordVPN conectada en $country"

# Clean sensitive vars from current shell process environment (best effort)
unset NORDVPN_PASSWORD || true
unset NORDVPN_TOKEN || true
unset password || true
unset token || true
