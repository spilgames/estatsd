.PHONY: test

REBAR=rebar
all: compile

compile:
	${REBAR} compile

clean:
	${REBAR} clean

test:
	${REBAR} compile eunit

docs:
	${REBAR} doc

# While developing with vi, :!make dialyzer | grep '%:t' can be used to run dialyzer in the current file
dialyzer: clean compile
	@dialyzer -Wno_return -Wno_opaque -c ebin

# Runs estatsd erlang application.
go:
	erl -pa ebin -s estatsd_app -config conf/estatsd -sname estatsd_demo

# For testing purposes starts a dummy tcp server based on netcat (nc).
graphite:
	./priv/graphite.sh
