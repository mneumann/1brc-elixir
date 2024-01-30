MEASUREMENTS?=${HOME}/measurements.txt

run:
	#/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}" Elixir.OBRC.Store.ETS.Unencoded 
	/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}" Elixir.OBRC.Store.ProcessDict

run-explorer:
	/usr/bin/time mix run --no-mix-exs 1brc-explorer.exs "${MEASUREMENTS}"

format:
	mix format 1brc.exs 1brc-explorer.exs
