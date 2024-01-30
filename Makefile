MEASUREMENTS?=${HOME}/measurements.txt

run:
	/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}" Elixir.OBRC.Store.Adaptive=0:Elixir.OBRC.Store.ProcessDict:414:Elixir.OBRC.Store.ETS
	#/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}" Elixir.OBRC.Store.ETS.Unencoded 
	#/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}" Elixir.OBRC.Store.ProcessDict
	#/usr/bin/time mix run --no-mix-exs 1brc.exs "${MEASUREMENTS}" Elixir.OBRC.Store.Map

run-explorer:
	/usr/bin/time mix run --no-mix-exs 1brc-explorer.exs "${MEASUREMENTS}"

format:
	mix format 1brc.exs 1brc-explorer.exs
