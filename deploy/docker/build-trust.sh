#!/bin/sh
# Optional build-time support for TLS-intercepting / corporate proxies (no-op otherwise).
#   * BuildKit secret "ca" (PEM, one or many certs) is trusted by the OS store, the JDK and Node/pip.
#   * HTTPS_PROXY (build arg) is translated for Gradle, which ignores the env var.
# Usage inside a RUN step:  RUN --mount=type=secret,id=ca,required=false realy-build-trust && <build>
set -e
CA=/run/secrets/ca
if [ -s "$CA" ]; then
  if [ -d /usr/local/share/ca-certificates ] && command -v update-ca-certificates >/dev/null 2>&1; then
    cp "$CA" /usr/local/share/ca-certificates/realy-build-ca.crt && update-ca-certificates >/dev/null 2>&1 || true
  elif [ -f /etc/ssl/certs/ca-certificates.crt ]; then
    cat "$CA" >> /etc/ssl/certs/ca-certificates.crt
  fi
  if command -v keytool >/dev/null 2>&1; then
    tmp=$(mktemp -d)
    awk -v d="$tmp" '/BEGIN CERTIFICATE/{n++} n{print > (d "/c" n ".pem")}' "$CA"
    for f in "$tmp"/c*.pem; do
      [ -s "$f" ] || continue
      keytool -importcert -noprompt -cacerts -storepass changeit -alias "realy-build-$(basename "$f" .pem)" \
        -file "$f" >/dev/null 2>&1 || true
    done
    rm -rf "$tmp"
  fi
fi
if [ -n "$HTTPS_PROXY" ] && command -v gradle >/dev/null 2>&1; then
  hp=${HTTPS_PROXY#*://}; hp=${hp%%/*}; host=${hp%:*}; port=${hp##*:}
  mkdir -p "${GRADLE_USER_HOME:-$HOME/.gradle}"
  {
    echo "systemProp.https.proxyHost=$host"; echo "systemProp.https.proxyPort=$port"
    echo "systemProp.http.proxyHost=$host";  echo "systemProp.http.proxyPort=$port"
    echo "systemProp.http.nonProxyHosts=localhost|127.0.0.1"
  } >> "${GRADLE_USER_HOME:-$HOME/.gradle}/gradle.properties"
fi
exit 0
