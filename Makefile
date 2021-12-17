##
# Erflux (the new version) for automation.
##

all:	deps build test

deps:
	rebar get-deps

build:	deps
	rebar compile

test:	build
	@cd apps/erflux/test && erlc test.erl && erl -noinput -noshell -pa ../../../_build/default/lib/*/ebin -s test -eval 'init:stop()'

clean:
	@rm -f *.dump *.lock *.beam
	@rm -f apps/erflux/test/test.beam
	@rm -rf _build
