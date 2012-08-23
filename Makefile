test:
	@./node_modules/.bin/mocha -u tdd --compilers coffee:coffee-script

.PHONY: test
