#!/bin/bash
# Toggle between OmniRoute and Direct Anthropic API for Claude Code
# Usage: ./toggle_claude_route.sh [omniroute|direct|status]

CONFIG_DIR="$HOME/.claude"
OMNIROUTE_CONFIG="$CONFIG_DIR/settings.local.omniroute.json"
ACTIVE_CONFIG="$CONFIG_DIR/settings.local.json"
BACKUP_CONFIG="$CONFIG_DIR/settings.local.direct.json"

show_status() {
    if [[ -f "$ACTIVE_CONFIG" ]] && grep -q "localhost:20128" "$ACTIVE_CONFIG" 2>/dev/null; then
        echo "📡 Current: OmniRoute (localhost:20128)"
    elif [[ -f "$ACTIVE_CONFIG" ]] && grep -q "ANTHROPIC_BASE_URL" "$ACTIVE_CONFIG" 2>/dev/null; then
        echo "📡 Current: Custom base URL ($(grep ANTHROPIC_BASE_URL "$ACTIVE_CONFIG" | head -1 | sed 's/.*: "\(.*\)".*/\1/'))"
    else
        echo "📡 Current: Direct Anthropic API (uses shell ANTHROPIC_API_KEY)"
    fi
}

use_omniroute() {
    if [[ ! -f "$OMNIROUTE_CONFIG" ]]; then
        echo "❌ OmniRoute config not found at $OMNIROUTE_CONFIG"
        exit 1
    fi
    cp "$OMNIROUTE_CONFIG" "$ACTIVE_CONFIG"
    echo "✅ Switched to OmniRoute (localhost:20128)"
    show_status
}

use_direct() {
    # Backup current if it's omniroute
    if [[ -f "$ACTIVE_CONFIG" ]] && grep -q "localhost:20128" "$ACTIVE_CONFIG" 2>/dev/null; then
        cp "$ACTIVE_CONFIG" "$BACKUP_CONFIG"
        echo "💾 Backed up OmniRoute config to $BACKUP_CONFIG"
    fi
    # Remove the active config so shell env vars take over
    rm -f "$ACTIVE_CONFIG"
    echo "✅ Switched to Direct Anthropic API"
    show_status
}

case "${1:-status}" in
    omniroute|omni|proxy)
        use_omniroute
        ;;
    direct|anthropic|claude)
        use_direct
        ;;
    status|show)
        show_status
        ;;
    *)
        echo "Usage: $0 [omniroute|direct|status]"
        echo ""
        echo "  omniroute  - Use OmniRoute proxy (localhost:20128)"
        echo "  direct     - Use direct Anthropic API (shell env vars)"
        echo "  status     - Show current routing (default)"
        exit 1
        ;;
esac