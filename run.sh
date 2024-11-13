#!/bin/sh

# Try bootnodes one by one
echo "$BOOTNODES" | while read -r bootnode; do
    # Skip empty lines
    [ -z "$bootnode" ] && continue
    
    WORKDIR="results/$(date -Iseconds)"
    echo $WORKDIR
    mkdir -p "$WORKDIR"
    git diff > $WORKDIR/git.diff
    git status > $WORKDIR/git.status
    git describe --always > $WORKDIR/git.describe
    cd "$WORKDIR"
    
    # Try current bootnode and capture output
    ../../build/dcrawl --bootnode="$bootnode" "$@" 2>&1 | tee output.log
    
    # Check if we see the failure message
    if ! grep -q "Message request to bootstrap node failed" output.log; then
        exit 0
    fi
    
    # If we get here, the connection failed - try next bootnode
    cd - > /dev/null
done

# If we get here, all bootnodes failed
exit 1