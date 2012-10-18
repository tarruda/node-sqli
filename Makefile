test:
	@./node_modules/.bin/mocha -u tdd --compilers coffee:coffee-script

compile:
	@./node_modules/.bin/coffee -o lib src

link: test compile
	npm link

pack: test compile
	npm pack


.PHONY: test
