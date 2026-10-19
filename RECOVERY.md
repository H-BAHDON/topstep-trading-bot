# Emergency halt and manual recovery

The bot starts with ENABLE_ORDER_EXECUTION=false and EMERGENCY_HALT=true.

If the bot reports NEW ORDERS HALTED, an uncertain entry or stop modification,
missing protection, stale market data, or a daily-loss halt:

1. Leave the bot stopped or set EMERGENCY_HALT=true.
2. In TopstepX, open the explicitly configured $150K PRACTICE account.
3. Check Positions for every open MNQ position.
4. Check Orders for pending entries, rejected changes, and the working protective Stop.
5. If an MNQ position exists, confirm its server-side stop is working. Manage or flatten
   it manually in TopstepX if the stop is missing or its price is uncertain.
6. Confirm realized P&L and that the daily loss limit has not been reached.
7. Confirm Settings > Risk Settings is set to Auto OCO Brackets.
8. Restart with ENABLE_ORDER_EXECUTION=false. Reconciliation must identify any existing
   position and its unique protective stop.
9. Resume enabled practice testing only after broker state and market data are healthy.

A restart clears the in-memory halt. The bot never blindly retries an entry or stop
modification after a timeout. The last confirmed broker stop remains active if the bot
or network fails, but trailing movement pauses until the bot reconnects.