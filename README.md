# ravenfall-multichat-server

Manages all my Ravenfall characters and does multi-account operations. Coded with AI assistance.  
Initial configuation is done in a `.env` file. This file must exist. There is an example in [.env.example](.env.example).

Depends on Python 3.12+ and `uv`. To run just execute `uv run main.py` in a terminal.  

The jank frontend that goes with this jank backend is at [ravenfall-multichat-client](https://github.com/abrokecube/ravenfall-multichat-client) :)

## Adding accounts
You must have `.env` already configured.  
In the twitch developer console make sure `http://localhost:4343/oauth/callback` is set as an OAuth redirect url.  
In your `.env` set `DISABLE_INTEGRATIONS` to `true`, then start the script.  
Login to the twitch account you want to add.  
If you want to be able to add moderators to the account, authenticate using [this link](http://localhost:4343/oauth?scopes=user:read:chat%20user:write:chat%20user:bot%20channel:manage:moderators), otherwise, authenticate with [this link](http://localhost:4343/oauth?scopes=user:read:chat%20user:write:chat%20user:bot).  
Tokens are placed in `.tio.tokens.json`. User configuration is done in `users.json`.

