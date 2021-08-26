module.exports = {
	'env': {
		'browser': true,
		'es6': true,
		'node': true,
		'mocha': true
	},
	'extends': 'eslint:recommended',
	'parserOptions': {
		'sourceType': 'module'
	},
	'rules': {
		'prefer-const': 1,
		'no-var': 1,
		'no-console': 0,
		'no-unused-vars': 0,
		'indent': [
			'error',
			'tab'
		],
		'linebreak-style': [
			'error',
			'unix'
		],
		'quotes': [
			'error',
			'single'
		],
		'semi': [
			'error',
			'always'
		]
	}
};