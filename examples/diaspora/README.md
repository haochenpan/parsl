# Diaspora Examples CLI

This directory uses a single entrypoint CLI:

- `diaspora.py`: command surface for setup, context, and clear.
- `topic_ops.py`: setup/context/clear handlers.

## Usage

```bash
# from repo root
cd examples/diaspora

# one-time setup
python diaspora.py setup

# fetch context messages from the last 24 hours
python diaspora.py context --time-horizon $(( ($(date +%s) - 86400) * 1000 ))

# fetch context messages from the last 5 minutes
python diaspora.py context --time-horizon $(( ($(date +%s) - 300) * 1000 ))

# recreate topic
python diaspora.py clear --topic my-topic
```
