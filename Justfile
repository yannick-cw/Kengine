watch:
	ghciwatch --command "cabal repl" --watch app --watch src

watch-test:
	ghciwatch --command "cabal repl kengine-test" --watch test

test:
	cabal test

testM TEST:
	cabal test --test-options='--match "{{TEST}}"'
