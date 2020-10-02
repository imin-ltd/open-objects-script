/**
 * Axios errors have a gargantuan amount of barely relevant data that makes
 * them unserializable. Here, it is simplified so that it can be logged.
 *
 * @param {import('axios').AxiosError} error
 */
function loggableAxiosError(error) {
  return {
    message: error.message,
    url: error.config.url,
    response: error.response && {
      status: error.response.status,
      headers: error.response.headers,
      data: error.response.data,
    },
  };
}

module.exports = {
  loggableAxiosError,
};
