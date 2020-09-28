module.exports = {
  env: {
    browser: true,
    commonjs: true,
    es2021: true,
  },
  extends: [
    'airbnb-base',
  ],
  parserOptions: {
    ecmaVersion: 12,
  },
  rules: {
    'max-len': 'off',
    'no-await-in-loop': 0,
    'no-restricted-syntax': 0,
    'no-continue': 0,
    'function-paren-newline': 0,
    'no-underscore-dangle': 0,
  },
};
