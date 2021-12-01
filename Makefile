##
# Erflux (the new version) for automation.
##

all:	deps build test

deps:
	rebar get-deps

build:	deps
	rebar compile

test:	build
	@cd apps/erflux/test && erlc test.erl && erl -noshell -pa ../../../_build/default/lib/*/ebin -s test

clean:
	@rm -f *.dump *.lock *.beam
	@rm -rf _build
