/* eslint-disable max-classes-per-file */
class IminValidationError extends Error { }

class Http404Error extends Error {
  /**
   * @param {string} url
   */
  constructor(url) {
    super(`Page not found: "${url}"`);
    this.url = url;
  }
}

module.exports = {
  Http404Error,
  IminValidationError,
};
