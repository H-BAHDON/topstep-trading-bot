# Account configuration

Set `ACCOUNTS_FILE=accounts.json` in `.env`. Relative paths resolve beside the
environment file defining that setting. Keep login/API credentials in `.env`.
The account file and generated account trade ledgers are ignored by Git.

Each account requires a unique positive integer `account_id`, a unique display
`name`, an exact broker `account_name`, a boolean `enabled`, and an `account_type`
of `practice` or `express`. The existing migrated practice entry is compatible:
its `verified_practice_account_name` supplies the broker name, and its type
defaults to `practice`.

Example Express entry (replace both placeholders with the broker identity):

```json
{
  "accounts": [
    {
      "name": "Express 1",
      "account_id": 10001,
      "account_name": "EXACT_BROKER_ACCOUNT_NAME",
      "account_type": "express",
      "enabled": true,
      "contracts": 1,
      "daily_loss_limit_enabled": true,
      "max_daily_loss_usd": "100"
    }
  ]
}
```

Append entries to the `accounts` list to add accounts. Restart to apply changes.
Optional sizing and loss settings override the strategy/environment defaults
for that account. All accounts share the selected strategy and market feed.

`enabled: false` prevents new entries while retaining position reconciliation,
protective/trailing stop management, and strategy exits. Keep an account in the
file while it has an open trade. Removing it removes the bot's management of it.
All configured accounts, including disabled ones, must resolve to their exact
active broker identities at startup. A file with all accounts disabled can run
to manage existing trades.

Express entries additionally require `PRACTICE_ONLY=false` in `.env` and
fresh broker-reported simulated-status verification through authenticated REST,
with the user hub used when REST omits that flag. Express
accounts have no manual verification fallback. Live Funded Accounts remain
unsupported. The existing explicit practice verification fallback is preserved
only for its matching configured practice identity.

Signals are submitted independently to each eligible account. A rejection,
uncertain order, or daily-loss halt on one account does not prevent other
accounts from receiving the signal. Fills and trade results can differ.
Each account reconciles broker positions/orders on restart and keeps its own
order state, P&L, sizing, and protective stops. Uncertain entries are not blindly
retried. Monitoring tasks finish before account locks and the API are released.

Telegram notifications and report replies include account names and IDs.
There is one command poller and a separate event queue and trade ledger for
each account. Commands sent in private chats can target a full configured name
(including spaces) or broker account ID. Matching is case insensitive; ambiguous
names are rejected, so use the numeric ID in that case.

Owner-only controls (the owner is `TELEGRAM_CHAT_ID` in `.env`):

| Command | Behavior |
| --- | --- |
| `/accounts` | List entry status, halts, and last-known open contract quantities. |
| `/pause Express 1` | Persist a pause and stop new entries; keep managing open trades. |
| `/resume Express 1` | Recheck identity, permissions, pending orders, protection, and daily loss before enabling entries. |
| `/close Express 1` | Persist a pause, close the strategy-contract position, reconcile exits, and remain paused. |
| `/settings Express 1` | Show that account's current runtime settings. |
| `/contracts Express 1 2` | Set contract size for future entries on that account. |
| `/trail_activation Express 1 10` | Set activation points for future entries. |
| `/trail_distance Express 1 5` | Set trailing distance points for future entries. |

Runtime sizing and trailing changes reset on restart; edit the account or
strategy configuration for permanent changes. With one account, existing
commands such as `/contracts 2` remain supported. Multiple accounts require an
explicit target for setting changes. Control commands never implicitly target
all accounts. `/stop` still means unsubscribe from notifications; it does not
pause trading.

All accounts share a REST request budget, with read pacing, reserved capacity
for order mutations, and a common cooldown after HTTP 429 responses. Uncertain
order submissions are still not retried. Other clients using the same broker
credentials can also consume provider capacity.

The effective background poll interval is the larger of
`EXECUTION_POLL_INTERVAL_SECONDS` and `1.5 * number_of_configured_accounts`
seconds (4.5 seconds with three accounts). This reduces request volume at the
cost of slower detection of trailing activation and position closures. The
broker's accepted protective orders remain independent of polling. Disabled
daily loss limits do not trigger background trade-history requests; enabled
limits refresh that history at most every 15 seconds. Pre-entry loss checks
still request fresh trade history.

Read reports (`/status`, `/profit`, `/trades`, `/stats`) keep their existing
public private-chat access and can specify an account or `all`. Without a
target, they report every account. `/profit all` also reports combined open,
daily realised, and total realised P&L. Combined realised totals are unavailable
if any account's broker data cannot be refreshed.

Pauses are stored by broker account ID in `runtime/account_controls.db` and
restored before startup reconciliation, even if Telegram is later disabled.
Preserve this file across deployments. A pause does not override `enabled:
false` in account configuration; enable the configuration and restart before
resuming such an account. Resume never clears a safety halt. If saving fails,
entries remain blocked in memory and Telegram reports that the pause may not
survive a restart.

Close operates only on the bot's configured strategy contract (currently MNQ).
It does not flatten unrelated instruments or blindly cancel untracked orders.
An uncertain close is not automatically resubmitted. The bot reports unresolved
positions/orders and leaves the account paused for broker review.

Legacy deployments without `ACCOUNTS_FILE` can continue using the single-account
practice identity settings in `.env`.
