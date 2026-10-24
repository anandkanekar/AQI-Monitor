#!/bin/sh
# Renders alertmanager.tmpl.yml by substituting ${VAR} placeholders from the environment,
# then starts Alertmanager. Keeps secrets (Slack webhook, SMTP password) out of git.
set -eu

TEMPLATE=/etc/alertmanager/alertmanager.tmpl.yml
CONFIG=/tmp/alertmanager.yml

cp "$TEMPLATE" "$CONFIG"
for var in SMTP_SMARTHOST SMTP_FROM SMTP_USERNAME SMTP_PASSWORD SMTP_REQUIRE_TLS \
           SLACK_WEBHOOK_URL SLACK_CHANNEL_AQI SLACK_CHANNEL_PLATFORM ALERT_EMAIL_TO SMS_WEBHOOK_URL; do
  eval "value=\${$var:-}"
  # Escape characters that are special in the sed replacement / YAML double-quoted strings.
  escaped=$(printf '%s' "$value" | sed -e 's/[\\|&]/\\&/g' -e 's/"/\\\\"/g')
  sed -i "s|\${$var}|$escaped|g" "$CONFIG"
done

/bin/amtool check-config "$CONFIG"
exec /bin/alertmanager --config.file="$CONFIG" --storage.path=/alertmanager "$@"
